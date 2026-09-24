import { cp, mkdir } from 'node:fs/promises';
await mkdir('dist/vendor', {recursive:true});
await cp('public', 'dist', {recursive:true});
await cp('node_modules/hls.js/dist/hls.min.js', 'dist/vendor/hls.min.js');
