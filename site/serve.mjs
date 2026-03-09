import { createServer } from 'http';
import { readFileSync, existsSync, statSync } from 'fs';
import { join, extname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const distDir = join(__dirname, 'dist');
const port = 4321;

const mimeTypes = {
  '.html': 'text/html',
  '.css': 'text/css',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
  '.woff': 'font/woff',
};

createServer((req, res) => {
  let url = req.url.replace('/FreshDataPlatform/', '/').replace('/FreshDataPlatform', '/');
  if (url === '/') url = '/index.html';
  if (!extname(url)) url = url.endsWith('/') ? url + 'index.html' : url + '/index.html';

  const filePath = join(distDir, url);
  if (existsSync(filePath) && statSync(filePath).isFile()) {
    const ext = extname(filePath);
    res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'application/octet-stream' });
    res.end(readFileSync(filePath));
  } else {
    const notFound = join(distDir, '404.html');
    res.writeHead(404, { 'Content-Type': 'text/html' });
    res.end(existsSync(notFound) ? readFileSync(notFound) : 'Not Found');
  }
}).listen(port, '0.0.0.0', () => {
  console.log(`Serving dist/ at http://localhost:${port}/FreshDataPlatform/`);
});
