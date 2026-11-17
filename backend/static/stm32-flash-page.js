import { BASE, FLASH_SIZE, hex, inspectBinary, Stm32Dfu } from './stm32-dfu.js';
const $ = id => document.getElementById(id);
let connection, fileBytes, fileName, busy = false, backedUp = false;
const lines = [];
const supported = window.isSecureContext && !!navigator.usb;
const serialSupported = window.isSecureContext && !!navigator.serial;
function log(message) {
  lines.push(`${new Date().toLocaleTimeString('vi-VN')} · ${message}`);
  $('log').textContent = lines.join('\n'); $('log').scrollTop = $('log').scrollHeight;
}
function status(message, kind = '') { $('status').textContent = message; $('status').className = kind; }
function refresh() {
  $('connect').disabled = busy || !!connection || !supported;
  $('disconnect').disabled = busy || !connection;
  $('backup').disabled = busy || !connection;
  $('firmware').disabled = busy; $('confirm').disabled = busy;
  $('enterdfu').disabled = busy || !!connection || !serialSupported;
  $('ending').disabled = busy;
  $('flash').disabled = busy || !connection || !fileBytes || !backedUp || !$('confirm').checked;
}
$('enterdfu').addEventListener('click', () => task(async () => {
  const port = await navigator.serial.requestPort();
  let writer;
  try {
    await port.open({ baudRate: 115200 });
    if (!port.writable) throw new Error('Virtual COM không có luồng ghi.');
    writer = port.writable.getWriter();
    const suffix = { raw: '', lf: '\n', crlf: '\r\n' }[$('ending').value];
    await writer.write(new TextEncoder().encode(`dfu${suffix}`));
    log(`Đã gửi dfu qua Virtual COM (${3 + suffix.length} byte). Chưa xác nhận chuyển chế độ.`);
    status('Đã gửi dfu. Khi board hiện STM32 BOOTLOADER, bấm Kết nối USB DFU.');
  } finally {
    writer?.releaseLock();
    try { await port.close(); } catch { log('Cổng COM đã đóng hoặc biến mất khi board đổi chế độ.'); }
  }
}));
function download(bytes, name, type = 'application/octet-stream') {
  const url = URL.createObjectURL(new Blob([bytes], { type }));
  const link = document.createElement('a'); link.href = url; link.download = name;
  document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 30000);
}
async function hash(bytes) {
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, '0')).join('');
}
async function task(action) {
  if (busy) return;
  busy = true; refresh();
  try { await action(); }
  catch (error) {
    const text = error.name === 'NotFoundError' ? 'Chưa chọn thiết bị USB.' : error.message;
    log(`DỪNG: ${text}`); status(text, 'error');
    if (connection) { try { await connection.close(); } catch {} connection = undefined; }
    backedUp = false; $('confirm').checked = false;
  } finally { busy = false; refresh(); }
}
$('connect').addEventListener('click', () => task(async () => {
  // Keep requestDevice in the original click's user activation, before any await.
  const device = await navigator.usb.requestDevice({ filters: [{ vendorId: 0x0483, productId: 0xdf11 }] });
  const candidate = new Stm32Dfu(device, log, (label, done, total) => {
    $('progress').value = done / total * 100; $('progresslabel').textContent = `${label}: ${Math.round(done / total * 100)}%`;
  });
  try { await candidate.connect(); } catch (error) { try { await candidate.close(); } catch {} throw error; }
  connection = candidate; backedUp = false; $('confirm').checked = false;
  status('Đã kết nối DFU · chưa ghi dữ liệu.', 'success');
}));
$('disconnect').addEventListener('click', () => task(async () => {
  await connection.close(); connection = undefined; backedUp = false; $('confirm').checked = false;
  status('Đã ngắt kết nối.'); log('Đã đóng USB.');
}));
let selection = 0;
$('firmware').addEventListener('change', async () => {
  const current = ++selection; fileBytes = undefined; $('confirm').checked = false;
  $('fileinfo').textContent = 'Đang kiểm tra file…'; refresh();
  try {
    const file = $('firmware').files[0];
    if (!file) { $('fileinfo').textContent = 'Chưa chọn file.'; return; }
    if (!/\.bin$/i.test(file.name) || file.size > FLASH_SIZE) throw new Error('Chọn file .bin tối đa 512 KiB.');
    const bytes = new Uint8Array(await file.arrayBuffer()); const vectors = inspectBinary(bytes); const sha = await hash(bytes);
    if (current !== selection) return;
    fileBytes = bytes; fileName = file.name;
    const end = BASE + bytes.length;
    $('fileinfo').textContent = `${file.name} · ${bytes.length.toLocaleString('vi-VN')} byte\n${hex(BASE)} – ${hex(end - 1)}\nMSP ${hex(vectors.stack)} · Reset ${hex(vectors.reset)}\nSHA-256: ${sha}\nVector hợp lệ không chứng minh firmware phù hợp phần cứng.`;
    log(`Chọn ${file.name} · SHA-256 ${sha}`);
  } catch (error) { if (current === selection) $('fileinfo').textContent = error.message; }
  finally { refresh(); }
});
$('confirm').addEventListener('change', refresh);
$('backup').addEventListener('click', () => task(async () => {
  const active = connection; backedUp = false;
  status('Đang đọc sao lưu Flash…');
  const bytes = await active.read(BASE, FLASH_SIZE, 'Sao lưu');
  const sha = await hash(bytes);
  if (connection !== active) throw new Error('Thiết bị đã bị ngắt kết nối.');
  const name = `stm32f407ve-backup-${new Date().toISOString().replace(/[:.]/g, '-')}.bin`;
  download(bytes, name); backedUp = true;
  log(`Đã yêu cầu tải ${name} · SHA-256 ${sha}. Kiểm tra file đã được lưu.`);
  status('Đã đọc sao lưu. Kiểm tra file tải về trước khi nạp.', 'success');
}));
$('flash').addEventListener('click', () => task(async () => {
  if (!connection || !fileBytes || !backedUp || !$('confirm').checked) throw new Error('Cần kết nối, file BIN, sao lưu và xác nhận.');
  const active = connection, bytes = fileBytes;
  status('Đang nạp. Giữ nguyên cáp và nguồn…');
  log(`Bắt đầu nạp ${fileName} tại ${hex(BASE)}.`);
  const readback = await active.flash(bytes);
  if (connection !== active) throw new Error('Thiết bị đã bị ngắt kết nối.');
  log(`SHA-256 đọc lại: ${await hash(readback)}`);
  status('Đã ghi và đọc lại khớp. Đặt BOOT0 = 0, nhấn RESET để chạy.', 'success');
  backedUp = false; $('confirm').checked = false;
}));
$('savelog').addEventListener('click', () => download(lines.join('\n'), 'stm32-dfu-log.txt', 'text/plain;charset=utf-8'));
navigator.usb?.addEventListener('disconnect', event => {
  if (connection?.device === event.device) {
    connection = undefined; backedUp = false; $('confirm').checked = false;
    log('USB đã ngắt.'); status('USB đã ngắt. Kết nối lại để tiếp tục.', 'error'); refresh();
  }
});
window.addEventListener('beforeunload', event => { if (busy) { event.preventDefault(); event.returnValue = ''; } });
status(supported ? 'Chưa kết nối. Gửi dfu qua Virtual COM, rồi kết nối USB DFU.' : 'Cần Chrome/Edge hỗ trợ WebUSB, mở bằng HTTPS hoặc localhost.', supported ? '' : 'error');
refresh();
