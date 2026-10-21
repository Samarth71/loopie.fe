// Loopie signaling + static server.  Run:  npm i && node server.js
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000,HTML=path.join(__dirname,'loopie.html');
const srv=http.createServer((q,s)=>{
  if(q.url==='/'||q.url.startsWith('/?')){fs.readFile(HTML,(e,d)=>{s.writeHead(e?500:200,{'Content-Type':'text/html; charset=utf-8'});s.end(e?'loopie.html missing':d)})}
  else{s.writeHead(404);s.end()}
});
const wss=new WebSocketServer({server:srv,path:'/ws'});
const C=new Map(),G=new Map();let n=0;
// Same Wi-Fi = same public IP. Private/LAN addresses (running locally) share one group.
function ipKey(q){
  let ip=(q.headers['x-forwarded-for']||q.socket.remoteAddress||'').split(',')[0].trim().replace(/^::ffff:/,'');
  if(/^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|127\.|169\.254\.)/.test(ip)||ip==='::1'||/^(f[cd]|fe80)/i.test(ip))return 'lan';
  if(ip.includes(':'))ip=ip.split(':').slice(0,4).join(':'); // IPv6: same /64 = same network
  return 'ip:'+ip;
}
const join=(c,g)=>{c.groups.add(g);if(!G.has(g))G.set(g,new Set());G.get(g).add(c.id)};
const peersOf=c=>{const s=new Set();c.groups.forEach(g=>G.get(g)&&G.get(g).forEach(i=>i!==c.id&&s.add(i)));return [...s].map(i=>({peer:i,presence:C.get(i).presence}))};
const push=c=>c.ws.readyState===1&&c.ws.send(JSON.stringify({t:'peers',peers:peersOf(c)}));
function refresh(c){const s=new Set([c.id]);c.groups.forEach(g=>G.get(g)&&G.get(g).forEach(i=>s.add(i)));s.forEach(i=>C.get(i)&&push(C.get(i)))}
wss.on('connection',(ws,q)=>{
  const c={id:'p'+(++n).toString(36)+Math.random().toString(36).slice(2,6),ws,presence:{},groups:new Set()};
  C.set(c.id,c);join(c,ipKey(q));
  ws.send(JSON.stringify({t:'hello',id:c.id}));refresh(c);
  ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}
    if(m.t==='presence'){c.presence=m.p||{};refresh(c)}
    else if(m.t==='emit'){const d=m.data||{},to=C.get(d.to);if(to&&to.ws.readyState===1)to.ws.send(JSON.stringify({t:'ev',topic:m.topic,data:d,peer:c.id}))}
    else if(m.t==='code'){let k;do{k=String(1000+Math.floor(Math.random()*9000))}while(G.has('code:'+k));join(c,'code:'+k);ws.send(JSON.stringify({t:'code',code:k}));refresh(c)}
    else if(m.t==='join'){const g='code:'+String(m.code);if(!/^\d{4}$/.test(String(m.code))||!G.get(g)||!G.get(g).size)return ws.send(JSON.stringify({t:'err',msg:'Nobody is using that code'}));join(c,g);refresh(c)}
  });
  ws.on('close',()=>{C.delete(c.id);const aff=new Set();c.groups.forEach(g=>{const s=G.get(g);if(s){s.delete(c.id);s.forEach(i=>aff.add(i));if(!s.size)G.delete(g)}});aff.forEach(i=>C.get(i)&&push(C.get(i)))});
});
srv.listen(PORT,()=>console.log('Loopie running on http://localhost:'+PORT));
