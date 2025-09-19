/* compact variant */
const ts = (a,t)=>{
  for(let i=0,m=new Map(); i<a.length; i++){
    let n=t-a[i];
    if(m.has(n)) return [m.get(n), i];
    m.set(a[i], i);
  }
  return null;
};

// console.log(ts([2,7,11,15],9));
