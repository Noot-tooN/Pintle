// Uses only Hermit Node built-ins. Sends public registry names/versions to OSV;
// excludes local workspace crates and the unpublished Go module. Advisory data
// is evidence, not executable input. Network failure fails the check.
import { writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
const cargo = JSON.parse(execFileSync('./bin/cargo', ['metadata', '--locked', '--format-version=1'], {encoding:'utf8'}));
const queries=cargo.packages.filter(p=>p.source?.startsWith('registry+')).map(p=>({package:{name:p.name,ecosystem:'crates.io'},version:p.version}));
const go=execFileSync('./bin/go',['list','-m','-json','all'],{encoding:'utf8'});
// go list emits consecutive JSON objects, rather than a JSON array.
for(const block of go.trim().split(/\n(?=\{)/)){const p=JSON.parse(block);if(!p.Main&&p.Version)queries.push({package:{name:p.Path,ecosystem:'Go'},version:p.Version});}
const response=await fetch('https://api.osv.dev/v1/querybatch',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({queries}),signal:AbortSignal.timeout(30000)});
if(!response.ok)throw new Error(`OSV query failed: ${response.status}`);
const data=await response.json();
if(!Array.isArray(data.results)||data.results.length!==queries.length)throw new Error('Invalid OSV response');
const findings=data.results.flatMap((r,i)=>(r.vulns??[]).map(v=>({...queries[i],id:v.id})));
const report={checked_at:new Date().toISOString(),source:'https://osv.dev',packages:queries.length,findings};
const output=process.argv[2]??'test-results/security-audit.json';
writeFileSync(output,JSON.stringify(report,null,2)+'\n');
console.log(JSON.stringify(report));
if(findings.length)process.exitCode=1;
