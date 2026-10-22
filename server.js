// Loopie signaling + static server (scaled for ~100+ people per network).
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000,HTML=path.join(__dirname,'loopie.html');
const STATIC={'manifest.webmanifest':'application/manifest+json','sw.js':'application/javascript; charset=utf-8','icon-192.png':'image/png','icon-512.png':'image/png','maskable-512.png':'image/png','apple-touch-icon.png':'image/png','favicon-32.png':'image/png','logo.svg':'image/svg+xml'};
const srv=http.createServer((q,s)=>{
  const u=q.url.split('?')[0],name=u.replace(/^\//,'');
  if(u==='/'||u==='/index.html'){fs.readFile(HTML,(e,d)=>{s.writeHead(e?500:200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});s.end(e?'loopie.html missing':d)})}
  else if(u==='/health'){s.writeHead(200);s.end('ok')}
  else if(Object.prototype.hasOwnProperty.call(STATIC,name)){fs.readFile(path.join(__dirname,name),(e,d)=>{if(e){s.writeHead(404);return s.end()}const h={'Content-Type':STATIC[name],'Cache-Control':name==='sw.js'||name==='manifest.webmanifest'?'no-cache':'public, max-age=86400'};if(name==='sw.js')h['Service-Worker-Allowed']='/';s.writeHead(200,h);s.end(d)})}
  else{s.writeHead(404);s.end()}
});
const wss=new WebSocketServer({server:srv,path:'/ws',maxPayload:1<<20});
const C=new Map(),G=new Map(),shakes=[];let n=0;
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
// Coalesce updates: with 100 people one join would otherwise cause 100 pushes x 100 peers.
const dirty=new Set();let timer=null;
function mark(ids){ids.forEach(i=>dirty.add(i));if(!timer)timer=setTimeout(()=>{timer=null;const d=[...dirty];dirty.clear();d.forEach(i=>C.get(i)&&push(C.get(i)))},150)}
function refresh(c){const s=[c.id];c.groups.forEach(g=>G.get(g)&&G.get(g).forEach(i=>s.push(i)));mark(s)}
wss.on('connection',(ws,q)=>{
  const c={id:'p'+(++n).toString(36)+Math.random().toString(36).slice(2,6),ws,presence:{},groups:new Set(),hits:0,t:Date.now()};
  c.ip=ipKey(q);C.set(c.id,c);join(c,c.ip);
  ws.send(JSON.stringify({t:'hello',id:c.id}));refresh(c);
  ws.on('message',raw=>{
    const now=Date.now();if(now-c.t>1000){c.t=now;c.hits=0}if(++c.hits>400)return; // flood guard (relay chunks allowed)
    let m;try{m=JSON.parse(raw)}catch(e){return}
    if(m.t==='presence'){c.presence=m.p||{};refresh(c)}
    else if(m.t==='emit'){const d=m.data||{},to=C.get(d.to);if(to&&to.ws.readyState===1)to.ws.send(JSON.stringify({t:'ev',topic:m.topic,data:d,peer:c.id}))}
    else if(m.t==='shake'){const now2=Date.now();const live=shakes.filter(z=>now2-z.t<2000&&z.id!==c.id&&C.has(z.id));shakes.length=0;live.forEach(z=>shakes.push(z));
      const same=live.filter(z=>C.get(z.id).ip===c.ip),pool=same.length?same:(live.length===1?live:[]);
      if(pool.length){const o=pool[0];shakes.splice(shakes.indexOf(o),1);const oc=C.get(o.id),g='shake:'+c.id+o.id;join(c,g);join(oc,g);[c,oc].forEach(z=>z.ws.readyState===1&&z.ws.send(JSON.stringify({t:'shaken'})));refresh(c);refresh(oc)}else shakes.push({id:c.id,t:now2})}
    else if(m.t==='code'){let k,t=0;do{k=String(1000+Math.floor(Math.random()*9000))}while(G.has('code:'+k)&&++t<50);join(c,'code:'+k);ws.send(JSON.stringify({t:'code',code:k}));refresh(c)}
    else if(m.t==='join'){const g='code:'+String(m.code);if(!/^\d{4}$/.test(String(m.code))||!G.get(g)||!G.get(g).size)return ws.send(JSON.stringify({t:'err',msg:'Nobody is using that code'}));join(c,g);refresh(c)}
  });
  ws.on('close',()=>{C.delete(c.id);const aff=[];c.groups.forEach(g=>{const s=G.get(g);if(s){s.delete(c.id);s.forEach(i=>aff.push(i));if(!s.size)G.delete(g)}});mark(aff)});
  ws.on('error',()=>{});
});
srv.listen(PORT,()=>console.log('Loopie running on http://localhost:'+PORT));
