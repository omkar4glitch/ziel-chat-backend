export function cleanNumber(v){

if(v===null||v===undefined) return 0;

if(typeof v==="number") return v;

const cleaned = v.toString()
.replace(/[$,()%]/g,"")
.trim();

const num = Number(cleaned);

return isNaN(num)?0:num;
}
