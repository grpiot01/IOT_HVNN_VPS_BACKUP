// STM32 DfuSe protocol: ST AN3156. No mass erase, read-unprotect or option-byte writes.
export const BASE = 0x08000000;
export const FLASH_SIZE = 512 * 1024;
export const hex = n => `0x${n.toString(16).toUpperCase().padStart(8, '0')}`;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

export function parseMemory(text) {
  const match = /^@Internal Flash\s*\/0x([\da-f]+)\/([^/]+)\s*$/i.exec(text ?? '');
  if (!match) throw new Error('Không nhận được descriptor Internal Flash của STM32.');
  let address = parseInt(match[1], 16);
  const sectors = [];
  for (const part of match[2].split(',')) {
    const m = /^\s*(\d+)\*(\d+)([BKM])([a-g])\s*$/i.exec(part);
    if (!m) throw new Error('Descriptor vùng nhớ không hợp lệ.');
    const count = Number(m[1]);
    const size = Number(m[2]) * ({ B: 1, K: 1024, M: 1048576 })[m[3].toUpperCase()];
    if (m[4].toLowerCase() !== 'g' || count < 1 || count > 512 || size < 1) {
      throw new Error('Vùng Flash không đủ quyền đọc/xóa/ghi hoặc geometry không hợp lệ.');
    }
    for (let i = 0; i < count; i++) { sectors.push({ address, size }); address += size; }
  }
  const expected = [16384, 16384, 16384, 16384, 65536, 131072, 131072, 131072];
  if (parseInt(match[1], 16) !== BASE || sectors.length !== expected.length ||
      sectors.some((s, i) => s.size !== expected[i])) {
    throw new Error('Công cụ này chỉ nhận geometry Flash 512 KiB của F407VE.');
  }
  return sectors;
}

export function inspectBinary(bytes) {
  if (!(bytes instanceof Uint8Array) || bytes.length < 8 || bytes.length > FLASH_SIZE || bytes.length % 4) {
    throw new Error('Cần file BIN thô, từ 8 byte đến 512 KiB, kích thước chia hết cho 4.');
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const stack = view.getUint32(0, true), reset = view.getUint32(4, true);
  const validStack = (stack > 0x20000000 && stack <= 0x20020000) ||
    (stack > 0x10000000 && stack <= 0x10010000);
  if (!validStack || stack % 8 || !(reset & 1) || reset - 1 < BASE || reset - 1 >= BASE + bytes.length) {
    throw new Error('Vector table không phù hợp: cần BIN liên kết tại 0x08000000 cho F407VE.');
  }
  return { stack, reset, size: bytes.length };
}

export class Stm32Dfu {
  constructor(device, log = () => {}, progress = () => {}) {
    this.device = device; this.log = log; this.progress = progress;
    this.interfaceNumber = 0; this.transferSize = 1024; this.sectors = [];
  }
  setup(request, value = 0) {
    return { requestType: 'class', recipient: 'interface', request, value, index: this.interfaceNumber };
  }
  async input(request, value, length) {
    const r = await this.device.controlTransferIn(this.setup(request, value), length);
    if (r.status !== 'ok' || !r.data || r.data.byteLength !== length) throw new Error('USB đọc thiếu dữ liệu hoặc bị chặn.');
    return new Uint8Array(r.data.buffer, r.data.byteOffset, r.data.byteLength).slice();
  }
  async output(request, value = 0, data = new Uint8Array()) {
    const r = await this.device.controlTransferOut(this.setup(request, value), data);
    if (r.status !== 'ok' || r.bytesWritten !== data.length) throw new Error('USB ghi thiếu dữ liệu hoặc bị chặn.');
  }
  async status() {
    const r = await this.input(3, 0, 6);
    return { code: r[0], delay: r[1] + r[2] * 256 + r[3] * 65536, state: r[4] };
  }
  async idle() {
    let s = await this.status();
    if (s.state === 10) { await this.output(4); s = await this.status(); }
    if (s.state === 5 || s.state === 9) { await this.output(6); s = await this.status(); }
    if (s.code || s.state !== 2) throw new Error(`DFU chưa sẵn sàng (status ${s.code}, state ${s.state}). Hãy reset về DFU.`);
  }
  async downloaded() {
    const deadline = Date.now() + 120000;
    while (Date.now() < deadline) {
      const s = await this.status();
      if (s.code || s.state === 10) throw new Error(`DFU báo lỗi ${s.code}, state ${s.state}. Không tự mở khóa bảo vệ.`);
      if (s.state === 5) return;
      if (![3, 4].includes(s.state)) throw new Error(`DFU sai trạng thái sau ghi: ${s.state}.`);
      await pause(Math.max(1, Math.min(s.delay, 20000)));
    }
    throw new Error('DFU quá thời gian chờ; cần kết nối lại và kiểm tra firmware.');
  }
  async command(code, address) {
    const bytes = new Uint8Array(5); bytes[0] = code;
    new DataView(bytes.buffer).setUint32(1, address, true);
    await this.idle(); await this.output(1, 0, bytes); await this.downloaded();
  }
  async descriptor(type, index, length, language = 0) {
    const r = await this.device.controlTransferIn({ requestType: 'standard', recipient: 'device',
      request: 6, value: (type << 8) | index, index: language }, length);
    if (r.status !== 'ok' || !r.data || r.data.byteLength < 2) throw new Error('Không đọc được USB descriptor.');
    return new Uint8Array(r.data.buffer, r.data.byteOffset, r.data.byteLength).slice();
  }
  async string(index) {
    if (!index) throw new Error('Thiếu tên vùng nhớ trong USB descriptor.');
    const langs = await this.descriptor(3, 0, 255);
    if (langs.length < 4) throw new Error('Thiếu USB language descriptor.');
    const bytes = await this.descriptor(3, index, 255, langs[2] | (langs[3] << 8));
    return new TextDecoder('utf-16le').decode(bytes.slice(2, bytes[0]));
  }
  async connect() {
    if (this.device.vendorId !== 0x0483 || this.device.productId !== 0xdf11) throw new Error('Chỉ nhận STM32 ROM DFU 0483:DF11.');
    await this.device.open();
    const config = this.device.configurations[0];
    if (!config) throw new Error('Thiếu USB configuration.');
    if (this.device.configuration?.configurationValue !== config.configurationValue) await this.device.selectConfiguration(config.configurationValue);
    const header = await this.descriptor(2, 0, 9);
    if (header.length < 9) throw new Error('USB configuration descriptor bị thiếu.');
    const raw = await this.descriptor(2, 0, header[2] | (header[3] << 8));
    const candidates = []; let functional;
    for (let p = 0; p + 2 <= raw.length;) {
      const len = raw[p]; if (len < 2 || p + len > raw.length) throw new Error('USB descriptor bị hỏng.');
      if (raw[p + 1] === 4 && len >= 9 && raw[p + 5] === 0xfe && raw[p + 6] === 1 && raw[p + 7] === 2) {
        candidates.push({ number: raw[p + 2], alternate: raw[p + 3], name: raw[p + 8] });
      }
      if (raw[p + 1] === 0x21 && len === 9) functional = raw.slice(p, p + len);
      p += len;
    }
    if (!functional || (functional[2] & 3) !== 3) throw new Error('DFU không hỗ trợ cả đọc và ghi.');
    this.transferSize = functional[5] | (functional[6] << 8);
    if (this.transferSize < 8 || this.transferSize > 4096) throw new Error('DFU transfer size không được hỗ trợ.');
    let chosen;
    for (const c of candidates) {
      const name = await this.string(c.name);
      if (/^@Internal Flash\s*\//i.test(name)) { this.sectors = parseMemory(name); chosen = c; break; }
    }
    if (!chosen) throw new Error('Không thấy Internal Flash 512 KiB.');
    this.interfaceNumber = chosen.number;
    await this.device.claimInterface(chosen.number);
    await this.device.selectAlternateInterface(chosen.number, chosen.alternate);
    await this.idle();
    this.log(`DFU sẵn sàng · Flash 512 KiB · mỗi gói ${this.transferSize} byte. Mã chip cần xác nhận trên board.`);
  }
  range(address, length) {
    if (!Number.isInteger(address) || !Number.isInteger(length) || length < 1 || address < BASE || address + length > BASE + FLASH_SIZE) {
      throw new Error('Địa chỉ vượt vùng Flash cho phép.');
    }
  }
  async read(address, length, label = 'Đọc') {
    this.range(address, length);
    const bytes = new Uint8Array(length);
    for (let off = 0; off < length; off += this.transferSize) {
      // Explicit address for each block avoids any short-final-block address ambiguity.
      await this.command(0x21, address + off); await this.idle();
      const size = Math.min(this.transferSize, length - off);
      bytes.set(await this.input(2, 2, size), off); await this.idle();
      this.progress(label, Math.min(off + size, length), length);
    }
    return bytes;
  }
  async flash(bytes) {
    inspectBinary(bytes); this.range(BASE, bytes.length);
    if (!this.sectors.length) throw new Error('Chưa kiểm tra geometry Flash.');
    const sectors = this.sectors.filter(s => s.address < BASE + bytes.length);
    for (const [i, sector] of sectors.entries()) {
      this.log(`Xóa sector ${hex(sector.address)} (${sector.size / 1024} KiB).`);
      await this.command(0x41, sector.address);
      this.progress('Xóa sector', i + 1, sectors.length);
    }
    for (let off = 0; off < bytes.length; off += this.transferSize) {
      await this.command(0x21, BASE + off);
      await this.output(1, 2, bytes.slice(off, off + this.transferSize)); await this.downloaded();
      this.progress('Ghi', Math.min(off + this.transferSize, bytes.length), bytes.length);
    }
    const readback = await this.read(BASE, bytes.length, 'Đọc lại kiểm chứng');
    for (let i = 0; i < bytes.length; i++) if (bytes[i] !== readback[i]) throw new Error(`Đọc lại không khớp tại ${hex(BASE + i)}.`);
    this.log(`Đã ghi và đọc lại khớp ${bytes.length.toLocaleString('vi-VN')} byte. Chưa xác nhận chương trình chạy.`);
    return readback;
  }
  async close() {
    if (this.device.opened) await this.device.close();
  }
}
