const fs = require('fs');
const babel = require('@babel/core');

// ★ index.html.bak（JSXソース）から <script type="text/babel"> を抽出
const src = fs.readFileSync('index.html.bak', 'utf8');
const match = src.match(/<script type="text\/babel">([\s\S]*?)<\/script>/);
if (!match) { console.error('text/babel block not found in index.html.bak'); process.exit(1); }

const jsx = match[1];
console.log(`JSX source: ${(jsx.length/1024).toFixed(0)}KB`);

// ★ Babel compile (JSX → JS)
const result = babel.transformSync(jsx, {
  presets: ['@babel/preset-react'],
});
console.log(`Compiled JS: ${(result.code.length/1024).toFixed(0)}KB`);

// ★ app.js に書き出し
fs.writeFileSync('app.js', result.code);
console.log('app.js written');

// ★ terser圧縮（利用可能なら）
try {
  const { execSync } = require('child_process');
  execSync('npx terser app.js --compress passes=3,pure_getters=true --mangle toplevel=true --output app.js', { stdio: 'inherit' });
  const sz = fs.statSync('app.js').size;
  console.log(`Minified app.js: ${(sz/1024).toFixed(0)}KB`);
} catch(e) {
  console.warn('terser not available, using unminified app.js');
}

console.log('✅ Build complete');
