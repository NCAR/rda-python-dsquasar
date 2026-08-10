const pptxgen = require("pptxgenjs");
const p = new pptxgen();
p.layout = "LAYOUT_WIDE";           // 13.333 x 7.5
const W = 13.333, H = 7.5;

// ---- palette (NSF NCAR GDEX Brand template, 2026) ----
const INK   = "00357A";   // NSF NCAR dark blue - headers / strong text
const DEEP  = "0057C2";   // NSF NCAR medium blue - primary
const TEAL  = "0092DB";   // NSF NCAR link blue - secondary / links
const MID   = "011837";   // NSF NCAR deep navy - dark background
const AMBER   = "C25E00"; // Dark orange (from brand orange) - readable emphasis on light
const AMBERLT = "42C0FF"; // Bright cyan - accents on dark backgrounds
const MUTE  = "53565A";   // Cool Gray 11 - muted text
const LIGHT = "FFFFFF";
const TINT  = "DCEBF7";   // light blue - card tint
const TINT2 = "C7E4F5";   // deeper light blue tint
const LINE  = "A9D6F2";   // light blue hairline
const GREEN = "0097A7";   // NSF NCAR teal - accent on light
const MONO  = "Courier New";
const SERIF = "Poppins";  // brand font (headers) - embedded in official template
const SANS  = "Poppins";  // brand font (body) - embedded in official template
// ---- official NSF NCAR logos (from GDEX 2026 template) ----
const LOGO_COLOR = "nsf_ncar_logo_color.png"; // full-colour, for light backgrounds
const LOGO_WHITE = "nsf_ncar_logo_white.png"; // white text, for dark backgrounds
const LOGO_W = 2.05, LOGO_H = 0.56;           // aspect 1005:276
// ---- official GDEX lockup logos (from GDEX 2026 template) ----
const GDEX_COLOR = "gdex_lockup_color.png";   // full-colour, for light backgrounds
const GDEX_WHITE = "gdex_lockup_white.png";   // white text, for dark backgrounds
const GDEX_W = 1.9, GDEX_H = 0.96;            // aspect 1000:505

let n = 0;
function logo(s, dark) {
  s.addImage({ path: dark ? LOGO_WHITE : LOGO_COLOR,
    x: W-LOGO_W-0.4, y: 0.3, w: LOGO_W, h: LOGO_H });
}
function foot(s, dark) {
  n++;
  logo(s, dark);
  s.addText("dsquasar  \u2022  Quasar Backup Guide", {
    x:0.5, y:H-0.42, w:8, h:0.3, fontFace:SANS, fontSize:9,
    color: dark?"97999B":MUTE, align:"left", margin:0 });
  s.addText(String(n), {
    x:W-0.9, y:H-0.42, w:0.4, h:0.3, fontFace:SANS, fontSize:9,
    color: dark?"97999B":MUTE, align:"right", margin:0 });
}
function kicker(s, txt, color) {
  s.addText(txt.toUpperCase(), { x:0.5, y:0.42, w:9, h:0.3, fontFace:SANS,
    fontSize:12, bold:true, color:color||TEAL, charSpacing:2, margin:0 });
}
function title(s, txt) {
  s.addText(txt, { x:0.5, y:0.72, w:10.2, h:0.7, fontFace:SERIF,
    fontSize:32, bold:true, color:INK, margin:0 });
}
function circ(s, x, y, d, fill, glyph, gcolor, gsize) {
  s.addShape(p.ShapeType.ellipse, { x,y,w:d,h:d, fill:{color:fill},
    line:{type:"none"} });
  s.addText(glyph, { x, y, w:d, h:d, align:"center", valign:"middle",
    fontFace:SANS, fontSize:gsize||16, bold:true, color:gcolor||LIGHT, margin:0 });
}
// dark code strip with monospace content
function code(s, x, y, w, h, txt, fs) {
  s.addShape(p.ShapeType.roundRect, { x, y, w, h, rectRadius:0.05,
    fill:{color:MID}, line:{type:"none"} });
  s.addText(txt, { x:x+0.14, y, w:w-0.28, h, fontFace:MONO, fontSize:fs||11,
    color:"0097A7", margin:0, valign:"middle", lineSpacingMultiple:1.0 });
}
// two-column reference table with a coloured header row
function reftable(s, x, y, w, colW, rows, hdr, hcolor, kcolor, fs) {
  const rr = [hdr.map(t => ({ text:t, options:{ bold:true, color:LIGHT,
    fill:{color:hcolor}, fontFace: t === hdr[0] ? MONO : SANS } }))];
  rows.forEach((r,i) => {
    const bg = i%2 ? TINT : LIGHT;
    rr.push(r.map((t,j) => ({ text:t, options:{ fill:{color:bg},
      fontFace: j===0 ? MONO : SANS, bold: j===0,
      color: j===0 ? (kcolor||DEEP) : INK } })));
  });
  s.addTable(rr, { x, y, w, colW, rowH:0.4, fontFace:SANS, fontSize:fs||12,
    valign:"middle", border:{type:"solid", color:LINE, pt:0.5},
    margin:[0,0.07,0,0.07] });
}

// ============================================================ 1 TITLE
(() => {
  const s = p.addSlide(); s.background = { color: MID };
  s.addImage({ path: LOGO_WHITE, x:0.7, y:0.6, w:2.8, h:0.77 });
  s.addImage({ path: GDEX_WHITE, x:W-GDEX_W-0.6, y:0.7, w:GDEX_W, h:GDEX_H });
  s.addText("dsquasar", { x:0.7, y:2.35, w:9, h:1.4, fontFace:SERIF,
    fontSize:78, bold:true, color:LIGHT, margin:0 });
  s.addText("Backing Up the GDEX Archive onto Quasar", {
    x:0.72, y:3.7, w:11.5, h:0.6, fontFace:SANS, fontSize:24,
    color:"C3D7EE", margin:0 });
  s.addText([
    { text:"Gather \u2192 List \u2192 Tar \u2192 Transfer, tracked end-to-end in GDEXDB", options:{} }
  ], { x:0.72, y:4.35, w:11, h:0.5, fontFace:SANS, italic:true,
       fontSize:15, color:"C3D7EE", margin:0 });
  const chips = ["Backup & Disaster Recovery", "Batch Operations", "Updated 2026-08"];
  let cx = 0.72;
  chips.forEach(c => {
    const w = 0.28 + c.length*0.098;
    s.addShape(p.ShapeType.roundRect, { x:cx, y:5.35, w, h:0.5,
      rectRadius:0.1, fill:{color:"0B2A63"}, line:{color:"2C4E7D", width:1} });
    s.addText(c, { x:cx, y:5.35, w, h:0.5, align:"center", valign:"middle",
      fontFace:SANS, fontSize:12, bold:true, color:"C3D7EE", margin:0 });
    cx += w + 0.2;
  });
  s.addText([
    { text:"Zaihua Ji", options:{ bold:true } },
    { text:"   zji@ucar.edu", options:{} },
  ], { x:0.72, y:6.05, w:11, h:0.4, fontFace:SANS, fontSize:13, color:"C3D7EE", margin:0 });
  s.addText("Tars through  dsarch AQ  \u2022  uploads through  dsglobus  \u2022  scheduled by  dscheck", {
    x:0.72, y:6.5, w:11, h:0.4, fontFace:MONO, fontSize:12, color:"97999B", margin:0 });
})();

// ============================================================ 2 OVERVIEW
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Overview", TEAL); title(s, "What is dsquasar?");
  s.addText([
    { text:"dsquasar", options:{ bold:true, color:DEEP } },
    { text:" keeps a ", options:{} },
    { text:"tape copy of the whole GDEX archive", options:{ bold:true } },
    { text:". It finds Web and Saved files that have never been backed up, groups them into large tar files built by ", options:{} },
    { text:"dsarch AQ", options:{ bold:true, color:DEEP, fontFace:MONO } },
    { text:", then uploads many of those tars at once to the NCAR Quasar Globus end points with ", options:{} },
    { text:"dsglobus", options:{ bold:true, color:DEEP, fontFace:MONO } },
    { text:".", options:{} },
  ], { x:0.5, y:1.65, w:5.3, h:1.9, fontFace:SANS, fontSize:16,
       color:INK, lineSpacingMultiple:1.15, margin:0, valign:"top" });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:3.65, w:5.3, h:2.95,
    rectRadius:0.1, fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText("Driven entirely by GDEXDB flags", {
    x:0.75, y:3.85, w:4.9, h:0.4, fontFace:SANS, bold:true, fontSize:14,
    color:DEEP, margin:0 });
  s.addText([
    { text:"Nothing is configured per run: ", options:{} },
    { text:"dataset.backflag", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" and ", options:{} },
    { text:"dsgroup.backflag", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" say what to back up.", options:{} },
    { text:"\nAny file with ", options:{} },
    { text:"bid = 0", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" is not backed up yet and is picked up automatically.", options:{} },
    { text:"\nEvery tar file gets a ", options:{} },
    { text:"bfile", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" record whose status tracks it from list to tape.", options:{} },
    { text:"\nWork is resumable: a run that stops leaves the pending records for the next run.", options:{} },
  ], { x:0.75, y:4.3, w:4.85, h:2.2, fontFace:SANS, fontSize:13,
       color:INK, lineSpacingMultiple:1.12, margin:0, valign:"top" });

  const caps = [
    ["\u2315","Gather","find Web/Saved files still to back up"],
    ["\u2261","List","write dsarch input files, add bfile records"],
    ["\u25A3","Tar","bundle into >=5 GB tar files via dsarch"],
    ["\u2601","Transfer","dsglobus batches to gdex-quasar (+drdata)"],
    ["\u2713","Verify","compare sizes on tape, reset if wrong"],
    ["\u25A4","Report","statistics & email for specialists"],
  ];
  const gx0=6.25, gy0=1.65, cw=3.35, ch=1.55, gapx=0.25, gapy=0.2;
  caps.forEach((c,i)=>{
    const col=i%2, row=Math.floor(i/2);
    const x=gx0+col*(cw+gapx), y=gy0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:LINE, width:1},
      shadow:{type:"outer", color:"C3D7EE", blur:5, offset:2, angle:90, opacity:0.35} });
    circ(s, x+0.22, y+0.24, 0.62, i%2?TEAL:DEEP, c[0], LIGHT, 20);
    s.addText(c[1], { x:x+1.0, y:y+0.22, w:cw-1.15, h:0.4, fontFace:SANS,
      bold:true, fontSize:15, color:INK, margin:0, valign:"middle" });
    s.addText(c[2], { x:x+1.0, y:y+0.66, w:cw-1.15, h:0.78, fontFace:SANS,
      fontSize:12, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.05 });
  });
  foot(s);
})();

// ============================================================ 3 INSTALL & RESOURCES
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Get it & learn it", TEAL); title(s, "Install & Resources");
  const cards = [
    { c:DEEP,  ic:"\u2325", t:"GitHub Repository",
      d:"Source code, issues, and contributions.",
      cmd:"git clone https://github.com/\n  NCAR/rda-python-dsquasar.git",
      link:"github.com/NCAR/rda-python-dsquasar",
      url:"https://github.com/NCAR/rda-python-dsquasar" },
    { c:GREEN, ic:"\u25BC", t:"PyPI Package",
      d:"Install the released package with pip; pulls in rda_python_common and rda_python_dsarch.",
      cmd:"pip install rda_python_dsquasar",
      link:"pypi.org/project/rda-python-dsquasar",
      url:"https://pypi.org/project/rda-python-dsquasar" },
    { c:AMBER, ic:"\u25A4", t:"Built-in Help",
      d:"The full usage document ships with the package and is shown by -h.",
      cmd:"dsquasar -h\n# renders dsquasar.usg",
      link:"src/rda_python_dsquasar/dsquasar.usg",
      url:"https://github.com/NCAR/rda-python-dsquasar" },
  ];
  const y0=1.85, cw=3.94, ch=3.9, gapx=0.26;
  cards.forEach((c,i)=>{
    const x=0.5+i*(cw+gapx);
    s.addShape(p.ShapeType.roundRect, { x, y:y0, w:cw, h:ch, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:LINE, width:1},
      shadow:{type:"outer", color:"C3D7EE", blur:5, offset:2, angle:90, opacity:0.3} });
    s.addShape(p.ShapeType.rect, { x, y:y0, w:cw, h:0.14, fill:{color:c.c}, line:{type:"none"} });
    s.addShape(p.ShapeType.ellipse, { x:x+0.3, y:y0+0.42, w:0.62, h:0.62,
      fill:{color:c.c}, line:{type:"none"} });
    s.addText(c.ic, { x:x+0.3, y:y0+0.42, w:0.62, h:0.62, align:"center",
      valign:"middle", fontFace:SANS, bold:true, fontSize:20, color:LIGHT, margin:0 });
    s.addText(c.t, { x:x+1.05, y:y0+0.42, w:cw-1.25, h:0.62, fontFace:SANS,
      bold:true, fontSize:16, color:INK, margin:0, valign:"middle" });
    s.addText(c.d, { x:x+0.3, y:y0+1.24, w:cw-0.6, h:0.9, fontFace:SANS,
      fontSize:12.5, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.08 });
    code(s, x+0.3, y0+2.2, cw-0.6, 0.82, c.cmd, 10.5);
    s.addShape(p.ShapeType.line, { x:x+0.3, y:y0+ch-0.5, w:cw-0.6, h:0, line:{color:LINE, width:1} });
    s.addText([
      { text:"\u2192  ", options:{ color:c.c, bold:true } },
      { text:c.link, options:{ color:DEEP, bold:true, hyperlink:{ url:c.url } } },
    ], { x:x+0.3, y:y0+ch-0.44, w:cw-0.6, h:0.34, fontFace:MONO, fontSize:10.5,
      margin:0, valign:"middle" });
  });
  s.addText([
    { text:"three console commands:  ", options:{ bold:true, color:AMBER } },
    { text:"dsquasar", options:{ fontFace:MONO, bold:true } },
    { text:"  (Quasar backup),  ", options:{} },
    { text:"tacctar", options:{ fontFace:MONO, bold:true } },
    { text:"  (build TACC tar bundles),  ", options:{} },
    { text:"taccrec", options:{ fontFace:MONO, bold:true } },
    { text:"  (recover from TACC bundles).", options:{} },
  ], { x:0.5, y:6.55, w:12.33, h:0.4, fontFace:SANS, fontSize:13, color:INK,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 4 PIPELINE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "How it works", TEAL); title(s, "The Four-Stage Backup Pipeline");
  const stages = [
    ["1","GDEX Files","Web & Saved files with bid = 0 on disk or object store", DEEP],
    ["2","Input Lists","dsarch input files + bfile record, status 'N'", TEAL],
    ["3","Tar Files","built on gdex-glade by dsarch AQ -TO, status 'T'", GREEN],
    ["4","Quasar Tape","dsglobus transfer to gdex-quasar (+drdata), status 'A'", MID],
  ];
  const y=2.2, cw=2.75, ch=2.5, gap=0.42;
  let x=0.62;
  stages.forEach((st,i)=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color: i===3?MID:TINT}, line:{color: st[3], width:2} });
    circ(s, x+cw/2-0.35, y+0.28, 0.7, st[3], st[0], LIGHT, 24);
    s.addText(st[1], { x:x+0.1, y:y+1.08, w:cw-0.2, h:0.55, align:"center",
      fontFace:SANS, bold:true, fontSize:15, color: i===3?LIGHT:INK, margin:0, valign:"middle" });
    s.addText(st[2], { x:x+0.15, y:y+1.6, w:cw-0.3, h:0.8, align:"center",
      fontFace:SANS, fontSize:11.5, color: i===3?"C3D7EE":MUTE, margin:0, valign:"top",
      lineSpacingMultiple:1.05 });
    if (i<3){
      const ax = x+cw+0.02;
      s.addText("\u2192", { x:ax, y, w:gap-0.04, h:ch, align:"center",
        valign:"middle", fontFace:SANS, fontSize:26, bold:true, color:st[3], margin:0 });
    }
    x += cw+gap;
  });
  s.addText([
    { text:"-A 1", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:"                                   ", options:{} },
    { text:"-A 2", options:{ fontFace:MONO, bold:true, color:GREEN } },
    { text:"                                   ", options:{} },
    { text:"-A 4", options:{ fontFace:MONO, bold:true, color:AMBER } },
  ], { x:2.0, y:4.75, w:9.5, h:0.35, fontFace:MONO, fontSize:14, margin:0, align:"center" });
  s.addShape(p.ShapeType.roundRect, { x:0.55, y:5.35, w:12.25, h:1.35, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  circ(s, 0.8, 5.62, 0.8, DEEP, "\u2318", LIGHT, 22);
  s.addText([
    { text:"-A 3 is the workhorse.  ", options:{ bold:true, color:DEEP } },
    { text:"It runs stages 1 and 2 together: files listed this run are tarred immediately, and existing ", options:{} },
    { text:"'N'", options:{ fontFace:MONO, bold:true } },
    { text:" records left by an earlier run are picked up too. ", options:{} },
    { text:"-A 4", options:{ fontFace:MONO, bold:true } },
    { text:" then moves the tars to tape. Use ", options:{} },
    { text:"-A 7", options:{ fontFace:MONO, bold:true } },
    { text:" for the full chain in one run.", options:{} },
  ], { x:1.8, y:5.55, w:10.8, h:0.95, fontFace:SANS, fontSize:14, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.1 });
  foot(s);
})();

// ============================================================ 5 DATA MODEL
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Data model", TEAL); title(s, "What dsquasar Reads and Writes in GDEXDB");
  const cards = [
    ["dataset / dsgroup","Backup Flags","read", DEEP,
      ["backflag 'B' \u2014 Backup copy only","backflag 'D' \u2014 Backup + Drdata copy","backflag 'N' \u2014 do not back up","group flag 'P' inherits the dataset flag","dataset.pid / lockhost \u2014 the run lock"]],
    ["wfile / sfile","Data Files","read + update", TEAL,
      ["bid = 0 \u21D2 not backed up yet","bid = <bfile> \u21D2 inside that tar","data_size / checksum / timestamps re-verified on disk before tarring","locflag 'O' files are pulled from object storage first"]],
    ["bfile","Backup File Record","write", GREEN,
      ["one record per tar file","status 'N' \u2192 'T' \u2192 'A'","type 'B' or 'D'; dsids lists extra datasets","note holds the whole input-file listing \u2014 the manifest of what is inside"]],
  ];
  const y=1.7, cw=3.95, ch=4.5, gap=0.28; let x=0.5;
  cards.forEach(c=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:LIGHT}, line:{color:c[3], width:2},
      shadow:{type:"outer", color:"C3D7EE", blur:6, offset:2, angle:90, opacity:0.3} });
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:1.15, rectRadius:0.1,
      fill:{color:c[3]}, line:{type:"none"} });
    s.addShape(p.ShapeType.rect, { x, y:y+0.6, w:cw, h:0.55, fill:{color:c[3]}, line:{type:"none"} });
    s.addText(c[1], { x:x+0.25, y:y+0.14, w:cw-0.5, h:0.45, fontFace:SERIF,
      bold:true, fontSize:20, color:LIGHT, margin:0, valign:"middle" });
    s.addText([
      { text:c[0]+"   ", options:{ fontFace:MONO, fontSize:12, color:"DBE2E9" } },
      { text:c[2], options:{ fontFace:SANS, fontSize:12, bold:true, color:"FFFFFF" } },
    ], { x:x+0.25, y:y+0.62, w:cw-0.5, h:0.4, margin:0, valign:"middle" });
    s.addText(c[4].map((t,i)=>({ text:t, options:{ bullet:{code:"2022", indent:14},
      breakLine:true, paraSpaceAfter: i===c[4].length-1?0:8 } })),
      { x:x+0.3, y:y+1.4, w:cw-0.55, h:ch-1.6, fontFace:SANS, fontSize:13,
        color:INK, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    x += cw+gap;
  });
  s.addText([
    { text:"The bfile note is the recovery key:  ", options:{ bold:true, color:DEEP } },
    { text:"it records every member file, its size and checksum, so a tar can be re-listed, re-tarred, or restored without re-reading the archive.", options:{} },
  ], { x:0.5, y:6.45, w:12.3, h:0.4, fontFace:SANS, fontSize:13, color:MUTE,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 6 COMMAND ANATOMY
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "General usage", TEAL); title(s, "Anatomy of a Command");
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:1.65, w:12.3, h:0.95,
    rectRadius:0.08, fill:{color:MID}, line:{type:"none"} });
  s.addText([
    { text:"dsquasar ", options:{ color:LIGHT } },
    { text:"(-a | -t dNNNNNN\u2026) ", options:{ color:AMBERLT, bold:true } },
    { text:"[-A Bits] ", options:{ color:"F0A030", bold:true } },
    { text:"[-B|-D] ", options:{ color:"0097A7", bold:true } },
    { text:"[Run\u2026] ", options:{ color:"C3D7EE", bold:true } },
    { text:"[-e|-E]", options:{ color:"C3D7EE" } },
  ], { x:0.75, y:1.65, w:11.8, h:0.95, fontFace:MONO, fontSize:20, margin:0, valign:"middle" });

  const blocks = [
    ["Scope","required","Which datasets. -a walks every dataset with a Backup flag; -t takes IDs and accepts the SQL wildcard %.", AMBERLT, "1A658F"],
    ["Action","-A Bits","What to do, as a bitmask: 1 list, 2 tar, 4 transfer, 8 check, 16 stats. Defaults to 3.", AMBER, "6E8300"],
    ["Copies","-B | -D","Which end points. -B Backup only, -D Backup + Drdata. Default: both.", GREEN, "00797C"],
    ["Run control","-b -d -m -l -n -W -w -u","How it runs: background, PBS delay, processes, locking, dry run, worker slots.", TEAL, "1A658F"],
  ];
  const y=2.95, cw=2.94, ch=2.4, gap=0.24; let x=0.5;
  blocks.forEach(b=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:TINT}, line:{color:b[3], width:2} });
    s.addText(b[0], { x:x+0.24, y:y+0.2, w:cw-0.45, h:0.45, fontFace:SERIF,
      bold:true, fontSize:19, color:b[4], margin:0 });
    s.addShape(p.ShapeType.roundRect, { x:x+0.24, y:y+0.72, w:cw-0.48, h:0.4,
      rectRadius:0.08, fill:{color:b[3]}, line:{type:"none"} });
    s.addText(b[1], { x:x+0.24, y:y+0.72, w:cw-0.48, h:0.4, align:"center",
      valign:"middle", fontFace:MONO, bold:true, fontSize:11, color: b[3]===AMBERLT?MID:LIGHT, margin:0 });
    s.addText(b[2], { x:x+0.24, y:y+1.25, w:cw-0.48, h:1.05, fontFace:SANS,
      fontSize:12, color:INK, margin:0, valign:"top", lineSpacingMultiple:1.08 });
    x += cw+gap;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.6, w:12.3, h:1.05, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Notes   ", options:{ bold:true, color:DEEP } },
    { text:"either ", options:{} },
    { text:"-a", options:{ fontFace:MONO, bold:true } },
    { text:" or ", options:{} },
    { text:"-t", options:{ fontFace:MONO, bold:true } },
    { text:" must be given, otherwise the usage is shown  \u2022  ", options:{} },
    { text:"-a", options:{ fontFace:MONO, bold:true } },
    { text:" wins over ", options:{} },
    { text:"-t", options:{ fontFace:MONO, bold:true } },
    { text:" with a warning  \u2022  the run always chdir's to the shared Quasar work directory  \u2022  ", options:{} },
    { text:"-h", options:{ fontFace:MONO, bold:true } },
    { text:" prints the full usage document.", options:{} },
  ], { x:0.8, y:5.65, w:11.8, h:0.95, fontFace:SANS, fontSize:13, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.1 });
  foot(s);
})();

// ============================================================ 7 QUICK START
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Quick start", GREEN); title(s, "Six Commands That Cover Most Work");
  const ex = [
    ["See what is waiting","Counts only \u2014 gathers and reports, performs no backup.",
      "dsquasar -t d123456 -n", DEEP],
    ["Back up one dataset","Create the input lists and build the tar files.",
      "dsquasar -t d123456 -A 3", TEAL],
    ["Send the tars to tape","Batches built tars into >=90 GB dsglobus transfers.",
      "dsquasar -t d123456 -A 4", GREEN],
    ["The nightly production run","All flagged datasets, in the background, as a PBS batch job with email.",
      "dsquasar -a -A 3 -e -b -d PBS", AMBER],
    ["Report on everything","Backup statistics for all datasets, mailed to the specialist.",
      "dsquasar -a -A 16 -e", DEEP],
    ["Clear a stale lock","Unlock datasets left locked by a crashed run.",
      "dsquasar -t d123456 -u", MUTE],
  ];
  const y0=1.7, cw=6.05, ch=1.6, gapx=0.23, gapy=0.14; let i=0;
  ex.forEach(c=>{
    const col=i%2, row=Math.floor(i/2);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:c[3], width:1.5},
      shadow:{type:"outer", color:"C3D7EE", blur:4, offset:2, angle:90, opacity:0.25} });
    circ(s, x+0.22, y+0.2, 0.5, c[3], String(i+1), LIGHT, 15);
    s.addText(c[0], { x:x+0.85, y:y+0.16, w:cw-1.05, h:0.4, fontFace:SANS,
      bold:true, fontSize:15, color:INK, margin:0, valign:"middle" });
    s.addText(c[1], { x:x+0.85, y:y+0.55, w:cw-1.05, h:0.4, fontFace:SANS,
      fontSize:11.5, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.02 });
    code(s, x+0.22, y+ch-0.58, cw-0.44, 0.46, c[2], 12);
    i++;
  });
  s.addText([
    { text:"Everything is incremental.  ", options:{ bold:true, color:DEEP } },
    { text:"A file is a candidate only while ", options:{} },
    { text:"bid = 0", options:{ fontFace:MONO, bold:true } },
    { text:", so re-running a command never re-backs up work that already succeeded.", options:{} },
  ], { x:0.5, y:6.62, w:12.33, h:0.4, fontFace:SANS, fontSize:13, color:INK,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 8 BACKUP FLAGS & SCOPE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Backup scope", TEAL); title(s, "Backup Flags: What Gets Two Copies");
  // left: flag table
  reftable(s, 0.5, 1.7, 6.05, [1.15,4.9], [
    ["'B'","Quasar Backup only \u2014 endpoint gdex-quasar"],
    ["'D'","Backup + Drdata \u2014 also gdex-quasar-drdata"],
    ["'N'","Not backed up at all"],
    ["'P'","Group level only: inherit the dataset's flag"],
  ], ["Flag","Meaning in dataset.backflag / dsgroup.backflag"], DEEP);
  s.addText([
    { text:"Group flags win.  ", options:{ bold:true, color:DEEP } },
    { text:"If any top-level ", options:{} },
    { text:"dsgroup", options:{ fontFace:MONO, bold:true } },
    { text:" carries its own flag, dsquasar walks the dataset group by group and applies each group's flag; groups flagged ", options:{} },
    { text:"'N'", options:{ fontFace:MONO, bold:true } },
    { text:" are skipped. A dataset with no group flags is treated as one unit.", options:{} },
  ], { x:0.5, y:4.05, w:6.05, h:1.35, fontFace:SANS, fontSize:13, color:INK,
       margin:0, valign:"top", lineSpacingMultiple:1.1 });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.5, w:6.05, h:1.15, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Command-line override:  ", options:{ bold:true, color:DEEP } },
    { text:"-B", options:{ fontFace:MONO, bold:true } },
    { text:" or ", options:{} },
    { text:"-D", options:{ fontFace:MONO, bold:true } },
    { text:" narrows a run to just that copy \u2014 they are mutually exclusive, and giving neither works on both.", options:{} },
  ], { x:0.75, y:5.55, w:5.6, h:1.05, fontFace:SANS, fontSize:12.5, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.08 });

  // right: two endpoints diagram
  s.addShape(p.ShapeType.roundRect, { x:6.78, y:1.7, w:6.05, h:4.95, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText("Two Globus end points, one source", { x:7.05, y:1.9, w:5.5, h:0.4,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  // source box
  s.addShape(p.ShapeType.roundRect, { x:7.6, y:2.5, w:4.4, h:0.85, rectRadius:0.08,
    fill:{color:MID}, line:{type:"none"} });
  s.addText([
    { text:"gdex-glade\n", options:{ bold:true, color:LIGHT, fontFace:MONO, fontSize:14 } },
    { text:"tar files built on disk", options:{ color:"C3D7EE", fontSize:11 } },
  ], { x:7.6, y:2.5, w:4.4, h:0.85, align:"center", valign:"middle",
       fontFace:SANS, margin:0 });
  s.addText("\u2193", { x:9.3, y:3.4, w:1.0, h:0.45, align:"center", fontFace:SANS,
    fontSize:22, bold:true, color:DEEP, margin:0 });
  const eps = [
    ["gdex-quasar","Backup copy \u2014 always written", DEEP],
    ["gdex-quasar-drdata","Disaster-recovery copy \u2014 only for flag 'D'", GREEN],
  ];
  let ey = 3.92;
  eps.forEach(e=>{
    s.addShape(p.ShapeType.roundRect, { x:7.15, y:ey, w:5.3, h:1.05, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:e[2], width:2} });
    s.addText(e[0], { x:7.4, y:ey+0.12, w:4.9, h:0.4, fontFace:MONO, bold:true,
      fontSize:14, color:e[2], margin:0, valign:"middle" });
    s.addText(e[1], { x:7.4, y:ey+0.52, w:4.9, h:0.4, fontFace:SANS,
      fontSize:11.5, color:MUTE, margin:0, valign:"middle" });
    ey += 1.2;
  });
  s.addText([
    { text:"A 'D' backup writes the Drdata copy first, then the Backup copy; both must succeed before the local tar is deleted and the record is marked 'A'.", options:{} },
  ], { x:7.15, y:6.15, w:5.3, h:0.45, fontFace:SANS, fontSize:11.5, color:MUTE,
       margin:0, valign:"top", lineSpacingMultiple:1.05 });
  foot(s);
})();

// ============================================================ 9 ACTION FAMILY
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Actions", AMBER); title(s, "The -A Action Bitmask");
  const acts = [
    ["-A 1","Create Input","list Web/Saved files, add bfile records 'N'", DEEP],
    ["-A 2","Tar Backup","build >=5 GB tars from 'N' lists \u2192 'T'", TEAL],
    ["-A 3","Create + Tar","the default, and the normal production run", AMBER, true],
    ["-A 4","Transfer","dsglobus batches of 'T' tars to Quasar \u2192 'A'", GREEN],
    ["-A 6","Tar + Transfer","build then ship in one run", DEEP],
    ["-A 7","Full chain","list, tar, and ship end to end", TEAL],
    ["-A 8","Check","compare sizes on tape, reset bad ones to 'N'", GREEN],
    ["-A 16","Statistics","dump counts for done and pending work", MUTE],
  ];
  const y0=1.7, cw=3.0, ch=1.5, gapx=0.12, gapy=0.2; let i=0;
  acts.forEach(a=>{
    const col=i%4, row=Math.floor(i/4);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color: a[4]?a[3]:LIGHT}, line:{color:a[3], width: a[4]?0:1.5},
      shadow:{type:"outer", color:"C3D7EE", blur:4, offset:2, angle:90, opacity:0.25} });
    s.addText(a[0], { x:x+0.2, y:y+0.15, w:cw-0.4, h:0.42, fontFace:MONO,
      bold:true, fontSize:20, color: a[4]?LIGHT:a[3], margin:0 });
    s.addText(a[1], { x:x+0.2, y:y+0.55, w:cw-0.4, h:0.35, fontFace:SANS,
      bold:true, fontSize:13.5, color: a[4]?LIGHT:INK, margin:0 });
    s.addText(a[2], { x:x+0.2, y:y+0.9, w:cw-0.35, h:0.55, fontFace:SANS,
      fontSize:10.5, color: a[4]?"DBE2E9":MUTE, margin:0, valign:"top",
      lineSpacingMultiple:1.02 });
    i++;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.35, w:12.33, h:1.3, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"It really is a bitmask.  ", options:{ bold:true, color:DEEP } },
    { text:"3 = 1+2, 6 = 2+4, 7 = 1+2+4 \u2014 only those combinations are accepted. ", options:{} },
    { text:"1", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"8", options:{ fontFace:MONO, bold:true } },
    { text:" and ", options:{} },
    { text:"16", options:{ fontFace:MONO, bold:true } },
    { text:" are single-process bookkeeping actions; ", options:{} },
    { text:"2", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"3", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"4", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"6", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"7", options:{ fontFace:MONO, bold:true } },
    { text:" are the batch actions that get PBS cpus and multi-processing.", options:{} },
  ], { x:0.8, y:5.5, w:11.8, h:1.0, fontFace:SANS, fontSize:13.5, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.12 });
  foot(s);
})();

// ============================================================ 10 STATUS LIFECYCLE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Backup records", TEAL); title(s, "The bfile Status Lifecycle");
  const sts = [
    ["N","Input Files","the list exists, the tar does not yet", DEEP],
    ["T","Tarred","tar built on gdex-glade, not on tape yet", GREEN],
    ["A","Archived","on Quasar; the local tar has been deleted", MID],
  ];
  let x=1.35;
  sts.forEach((st,i)=>{
    s.addShape(p.ShapeType.roundRect, { x, y:1.85, w:3.1, h:1.75, rectRadius:0.1,
      fill:{color: i===2?MID:TINT}, line:{color:st[3], width:2} });
    circ(s, x+0.24, y=2.1, 0.72, st[3], "'"+st[0]+"'", i===2?AMBERLT:LIGHT, 20);
    s.addText(st[1], { x:x+1.1, y:2.08, w:1.85, h:0.4, fontFace:SANS, bold:true,
      fontSize:16, color: i===2?LIGHT:INK, margin:0, valign:"middle" });
    s.addText(st[2], { x:x+0.28, y:2.85, w:2.6, h:0.65, fontFace:SANS,
      fontSize:11.5, color: i===2?"C3D7EE":MUTE, margin:0, valign:"top",
      lineSpacingMultiple:1.05 });
    if (i<2) s.addText("\u2192", { x:x+3.1, y:1.85, w:0.6, h:1.75, align:"center",
      valign:"middle", fontFace:SANS, fontSize:26, bold:true, color:st[3], margin:0 });
    x += 3.7;
  });
  s.addText([
    { text:"-A 2 / -A 3", options:{ fontFace:MONO, bold:true, color:GREEN } },
    { text:"                                ", options:{} },
    { text:"-A 4", options:{ fontFace:MONO, bold:true, color:AMBER } },
  ], { x:2.5, y:3.65, w:8.5, h:0.35, fontFace:MONO, fontSize:13, margin:0, align:"center" });

  s.addText("Records also move backwards \u2014 that is the self-healing part", {
    x:0.5, y:4.15, w:12.3, h:0.35, fontFace:SANS, bold:true, fontSize:15,
    color:DEEP, margin:0 });
  const backs = [
    ["'T' \u2192 'N'","the local tar file is missing when a transfer is prepared, so the tar is rebuilt from the stored input lists", TEAL],
    ["'A' \u2192 'N'","-A 8 finds the file absent or the wrong size on the end point, so the whole tar is redone", GREEN],
    ["record dropped","dsarch reports every member file as already backed up elsewhere, so the duplicate placeholder is deleted", AMBER],
  ];
  let by = 4.6;
  backs.forEach(b=>{
    s.addShape(p.ShapeType.roundRect, { x:0.5, y:by, w:12.3, h:0.66, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:b[2], width:1.5} });
    s.addText(b[0], { x:0.75, y:by, w:2.6, h:0.66, fontFace:MONO, bold:true,
      fontSize:13, color:b[2], margin:0, valign:"middle" });
    s.addText(b[1], { x:3.4, y:by, w:9.2, h:0.66, fontFace:SANS, fontSize:12.5,
      color:INK, margin:0, valign:"middle" });
    by += 0.78;
  });
  s.addText([
    { text:"Because status drives everything, an interrupted run is never a problem: whatever is still ", options:{} },
    { text:"'N'", options:{ fontFace:MONO, bold:true } },
    { text:" or ", options:{} },
    { text:"'T'", options:{ fontFace:MONO, bold:true } },
    { text:" is simply picked up by the next run.", options:{} },
  ], { x:0.5, y:6.9, w:12.33, h:0.35, fontFace:SANS, fontSize:12.5, color:MUTE,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 11 -A 1 DEEP DIVE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Actions", AMBER); title(s, "-A 1 Deep Dive: Building the Input Lists");
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:1.7, w:6.0, h:4.9, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText("For each flagged dataset", { x:0.75, y:1.9, w:5.5, h:0.4,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  const steps = [
    "Lock the dataset, then re-gather its files under the lock",
    "Skip files already listed in an open 'N' record",
    "Re-check each file on disk or object storage; fix size, checksum and timestamp in GDEXDB if they drifted",
    "Append a line per file to a dsarch input file, one per dataset and file type",
    "Close the group once it is big enough and add the bfile placeholder record",
  ];
  s.addText(steps.map((t,i)=>({ text:t, options:{ bullet:{type:"number", indent:20},
    breakLine:true, paraSpaceAfter: i===steps.length-1?0:9 } })),
    { x:0.9, y:2.4, w:5.4, h:2.6, fontFace:SANS, fontSize:13.5, color:INK,
      margin:0, valign:"top", lineSpacingMultiple:1.1 });
  s.addText("Input file layout", { x:0.75, y:5.0, w:5.5, h:0.35,
    fontFace:SANS, bold:true, fontSize:14, color:DEEP, margin:0 });
  code(s, 0.75, 5.38, 5.55, 1.1,
    "DS<=>d123456\nWF<:>WT<:>SZ<:>MC<:>\nfile.nc<:>data<:>2147483648<:>3f1c\u2026<:>", 10.5);

  // right: size thresholds
  s.addText("When is a tar big enough?", { x:6.78, y:1.72, w:6.05, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  reftable(s, 6.78, 2.15, 6.05, [1.6,4.45], [
    ["20 GB","a single file alone is enough to be its own tar"],
    ["5 GB","normal target size while under 100 files"],
    ["2 GB","accepted minimum once 100 files have piled up"],
    ["65 datasets","hard cap on datasets sharing one tar file"],
  ], ["Threshold","Rule applied while accumulating files"], AMBER, AMBER);
  s.addShape(p.ShapeType.roundRect, { x:6.78, y:4.15, w:6.05, h:1.35, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Small files are the reason.  ", options:{ bold:true, color:DEEP } },
    { text:"Tape wants few large files, so many tiny files are packed together \u2014 across datasets if need be \u2014 while a single very large file is never split.", options:{} },
  ], { x:7.03, y:4.2, w:5.55, h:1.25, fontFace:SANS, fontSize:12.5, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.08 });
  s.addText("Resulting tar file name", { x:6.78, y:5.62, w:6.05, h:0.35,
    fontFace:SANS, bold:true, fontSize:14, color:DEEP, margin:0 });
  code(s, 6.78, 6.0, 6.05, 0.5, "G001/d123456_sn40321_dn3_fn218.tar", 12);
  s.addText("subpath / dsid _ bid _ dataset count _ file count", { x:6.78, y:6.55, w:6.05, h:0.3,
    fontFace:SANS, fontSize:11, color:MUTE, margin:0 });
  foot(s);
})();

// ============================================================ 12 -A 2 DEEP DIVE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Actions", AMBER); title(s, "-A 2 Deep Dive: Building the Tar Files");
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:1.7, w:12.3, h:1.0, rectRadius:0.08,
    fill:{color:MID}, line:{type:"none"} });
  s.addText([
    { text:"dsarch ", options:{ color:LIGHT } },
    { text:"d123456 ", options:{ color:"C3D7EE" } },
    { text:"AQ ", options:{ color:"F0A030", bold:true } },
    { text:"-QT D -QF ", options:{ color:AMBERLT } },
    { text:"G001/d123456_sn40321_dn3_fn218.tar ", options:{ color:"C3D7EE" } },
    { text:"-TO -OE -MD -IF ", options:{ color:"0097A7", bold:true } },
    { text:"d123456_W_40321.txt \u2026", options:{ color:"C3D7EE" } },
  ], { x:0.72, y:1.7, w:11.9, h:1.0, fontFace:MONO, fontSize:13, margin:0,
       valign:"middle", lineSpacingMultiple:1.1 });
  const flags = [
    ["AQ","Archive Quasar \u2014 pack the listed files into one tar", DEEP],
    ["-QT","copy type: 'B' Backup only, or 'D' Backup + Drdata", TEAL],
    ["-QF","the tar file name recorded in the bfile record", GREEN],
    ["-TO","Tar Only \u2014 build the tar, do not upload it", AMBER],
    ["-OE","overwrite an existing Quasar file of that name", DEEP],
    ["-MD","act on any specialist's dataset", MUTE],
    ["-IF","the input files listing what goes into the tar", TEAL],
  ];
  const y0=2.95, rh=0.5;
  flags.forEach((f,i)=>{
    const y = y0+i*rh;
    s.addShape(p.ShapeType.rect, { x:0.5, y, w:6.05, h:rh-0.04,
      fill:{color: i%2?TINT:LIGHT}, line:{color:LINE, width:0.5} });
    s.addText(f[0], { x:0.7, y, w:1.0, h:rh-0.04, fontFace:MONO, bold:true,
      fontSize:13, color:f[2], margin:0, valign:"middle" });
    s.addText(f[1], { x:1.75, y, w:4.65, h:rh-0.04, fontFace:SANS, fontSize:11.5,
      color:INK, margin:0, valign:"middle" });
  });
  // right column notes
  const notes = [
    ["One child per tar file","With -m N, each tar file is handed to its own child process, so tarring scales widely across cpus.", DEEP],
    ["Missing lists are rebuilt","The bfile note holds a copy of every input file, so a list deleted from the work directory is written back out before tarring.", TEAL],
    ["Verified before it counts","After the children finish, the confirmed counts are re-read from GDEXDB, so the summary reports work that actually reached status 'T'.", GREEN],
    ["Duplicates are dropped","If dsarch reports every member as already backed up under a different tar, the placeholder record and its lists are removed.", AMBER],
  ];
  let ny = 2.95;
  notes.forEach(nt=>{
    s.addShape(p.ShapeType.roundRect, { x:6.78, y:ny, w:6.05, h:0.85, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:nt[2], width:1.5} });
    s.addText(nt[0], { x:7.0, y:ny+0.06, w:5.65, h:0.32, fontFace:SANS, bold:true,
      fontSize:13, color:nt[2], margin:0, valign:"middle" });
    s.addText(nt[1], { x:7.0, y:ny+0.36, w:5.65, h:0.45, fontFace:SANS,
      fontSize:11, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.02 });
    ny += 0.95;
  });
  s.addText([
    { text:"In an -A 3 run both phases tar.  ", options:{ bold:true, color:DEEP } },
    { text:"A list created this run is tarred right away, and any 'N' record left over from before is tarred too.", options:{} },
  ], { x:0.5, y:6.6, w:12.33, h:0.4, fontFace:SANS, fontSize:13, color:INK,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 13 -A 4 DEEP DIVE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Actions", AMBER); title(s, "-A 4 Deep Dive: Transferring to Quasar");
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:1.7, w:6.0, h:4.9, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText("For each batch of tar files", { x:0.75, y:1.9, w:5.5, h:0.4,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  const steps = [
    "Walk the status 'T' records and accumulate tars until the batch reaches 90 GB",
    "Confirm each tar still exists on gdex-glade; if not, reset that record to 'N' to be rebuilt",
    "Hand the whole list to dsglobus as one transfer \u2014 Drdata first when the flag is 'D', then Backup",
    "Wait for both transfers to report finished",
    "Delete the local tars and set every record in the batch to status 'A'",
  ];
  s.addText(steps.map((t,i)=>({ text:t, options:{ bullet:{type:"number", indent:20},
    breakLine:true, paraSpaceAfter: i===steps.length-1?0:9 } })),
    { x:0.9, y:2.4, w:5.4, h:2.9, fontFace:SANS, fontSize:13.5, color:INK,
      margin:0, valign:"top", lineSpacingMultiple:1.1 });
  s.addShape(p.ShapeType.roundRect, { x:0.75, y:5.4, w:5.55, h:1.05, rectRadius:0.08,
    fill:{color:LIGHT}, line:{color:AMBER, width:1.5} });
  s.addText([
    { text:"All or nothing.  ", options:{ bold:true, color:AMBER } },
    { text:"If either copy fails, no local tar is deleted and no record advances \u2014 the whole batch is retried by a later run.", options:{} },
  ], { x:0.95, y:5.45, w:5.2, h:0.95, fontFace:SANS, fontSize:12, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.05 });

  // right: batching visual
  s.addText("Why batch to 90 GB?", { x:6.78, y:1.72, w:6.05, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  s.addShape(p.ShapeType.roundRect, { x:6.78, y:2.15, w:6.05, h:2.2, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  // 18 little tar blocks
  for (let i=0;i<18;i++){
    const col=i%9, row=Math.floor(i/9);
    s.addShape(p.ShapeType.roundRect, { x:7.05+col*0.6, y:2.45+row*0.5, w:0.5, h:0.38,
      rectRadius:0.04, fill:{color:GREEN}, line:{type:"none"} });
  }
  s.addText("18 tar files \u00d7 5 GB  =  one 90 GB dsglobus transfer", {
    x:7.05, y:3.5, w:5.5, h:0.35, fontFace:SANS, bold:true, fontSize:13,
    color:INK, margin:0 });
  s.addText("dsarch AQ can upload a tar as it builds it, but only one at a time. dsquasar instead calls dsglobus directly with the whole list, so a batch costs one transfer request rather than eighteen. Each batch is also the unit of parallelism \u2014 one child process per batch.", {
    x:7.05, y:3.85, w:5.5, h:0.6, fontFace:SANS, fontSize:11, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.03 });
  s.addText("Source and destination paths", { x:6.78, y:4.5, w:6.05, h:0.35,
    fontFace:SANS, bold:true, fontSize:14, color:DEEP, margin:0 });
  code(s, 6.78, 4.88, 6.05, 1.0,
    "from  /data/gdex-quasar/d123456/<tar>      (Web)\n      /decsdata/gdex-quasar/d123456/<tar>  (Saved)\nto    /d123456/G001/<tar>", 10.5);
  s.addShape(p.ShapeType.roundRect, { x:6.78, y:6.0, w:6.05, h:0.6, rectRadius:0.08,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Single worker only.  ", options:{ bold:true, color:DEEP } },
    { text:"Transfers claim tars without dataset locks, so -A 4 never runs two workers.", options:{} },
  ], { x:6.98, y:6.0, w:5.65, h:0.6, fontFace:SANS, fontSize:11.5, color:INK,
       margin:0, valign:"middle" });
  foot(s);
})();

// ================================================== 14 RE-BACKUP CHANGED FILES
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Changed Files", AMBER);
  title(s, "-c ChangeDays  \u2014  Re-Backing Up What Moved");
  s.addText("A file already on tape can be replaced on disk: a corrected version, a re-generated grid, a re-processed year. The tape copy is then stale. -c tells dsquasar to look past the \u201cnever backed up\u201d test and pick up files whose GDEXDB record changed recently.", {
    x:0.5, y:1.6, w:6.05, h:1.0, fontFace:SANS, fontSize:14.5, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.1 });
  code(s, 0.5, 2.75, 6.05, 0.55,
    "dsquasar -t d123456 -A 1 -c 30", 14);
  s.addText("Only meaningful with -A 1 \u2014 the gather step is what decides which files land in the input lists. Must be greater than 0.", {
    x:0.5, y:3.45, w:6.05, h:0.5, fontFace:SANS, fontSize:11.5, color:MUTE,
    margin:0, valign:"top" });

  s.addText("What changes", { x:0.5, y:4.05, w:6.05, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  const eff = [
    ["Selection", "adds files with a recent change date, even if they already have a backup record"],
    ["Tar name", "gets a  _changed  suffix so the re-backup is obvious on tape"],
    ["Old copy", "stays on Quasar \u2014 nothing is deleted; the new tar is an additional version"],
  ];
  let ey = 4.42;
  eff.forEach(([k,v]) => {
    s.addShape(p.ShapeType.roundRect, { x:0.5, y:ey, w:6.05, h:0.66,
      rectRadius:0.07, fill:{color:TINT}, line:{color:LINE, width:1} });
    s.addText(k, { x:0.68, y:ey, w:1.15, h:0.66, fontFace:SANS, bold:true,
      fontSize:12, color:INK, margin:0, valign:"middle" });
    s.addText(v, { x:1.88, y:ey, w:4.5, h:0.66, fontFace:SANS, fontSize:11,
      color:MUTE, margin:0, valign:"middle", lineSpacingMultiple:1.0 });
    ey += 0.76;
  });

  s.addText("The four-way gather", { x:6.9, y:1.6, w:5.95, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  s.addText("Each dataset is walked for up to four input lists, and -c widens each of them:", {
    x:6.9, y:1.95, w:5.95, h:0.4, fontFace:SANS, fontSize:11.5, color:MUTE, margin:0 });
  reftable(s, 6.9, 2.42, 5.95, [1.5, 4.45], [
    ["Web / new",   "Web files with no backup record"],
    ["Web / chg",   "Web files changed within ChangeDays"],
    ["Saved / new", "Saved files with no backup record"],
    ["Saved / chg", "Saved files changed within ChangeDays"],
  ], ["List", "Contents"], DEEP);

  s.addShape(p.ShapeType.roundRect, { x:6.9, y:4.65, w:5.95, h:1.15,
    rectRadius:0.08, fill:{color:MID}, line:{type:"none"} });
  s.addText("Pick ChangeDays to match your run cadence", {
    x:7.15, y:4.8, w:5.5, h:0.32, fontFace:SANS, bold:true, fontSize:13,
    color:AMBERLT, margin:0 });
  s.addText("A large window re-tars files that were already re-tarred on the previous run, burning tape for nothing. Roughly one gather interval plus a safety margin is the useful range.", {
    x:7.15, y:5.12, w:5.5, h:0.6, fontFace:SANS, fontSize:11, color:"C3D7EE",
    margin:0, valign:"top", lineSpacingMultiple:1.05 });

  s.addShape(p.ShapeType.roundRect, { x:6.9, y:6.0, w:5.95, h:0.62,
    rectRadius:0.08, fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Group flags still apply.  ", options:{ bold:true, color:DEEP } },
    { text:"A group with backflag 'N' is skipped even for changed files.", options:{} },
  ], { x:7.1, y:6.0, w:5.55, h:0.62, fontFace:SANS, fontSize:11.5, color:INK,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================== 15 -A 8 CHECK / -A 16 STATS
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Verify & Report", GREEN);
  title(s, "-A 8 Integrity Check  \u2022  -A 16 Statistics");
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:1.6, w:6.05, h:4.95,
    rectRadius:0.1, fill:{color:LIGHT}, line:{color:DEEP, width:1.5} });
  circ(s, 0.75, 1.85, 0.5, DEEP, "8");
  s.addText("-A 8  \u2014  does tape match the database?", {
    x:1.4, y:1.85, w:4.95, h:0.5, fontFace:SANS, bold:true, fontSize:15,
    color:INK, margin:0, valign:"middle" });
  s.addText("Walks every bfile record with status 'A' for the selected datasets, asks the Quasar end point for the real file size, and compares it against the size recorded in GDEXDB.", {
    x:0.75, y:2.5, w:5.6, h:0.75, fontFace:SANS, fontSize:12.5, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  code(s, 0.75, 3.32, 5.6, 0.5, "dsquasar -a -A 8 -e", 13);
  const chk = [
    ["sizes agree", "record is left alone; the tape copy is trusted"],
    ["sizes differ", "status is reset 'A' \u2192 'N' and the file is re-listed, re-tarred and re-sent on the next -A 3 / -A 4"],
    ["file missing", "same reset \u2014 the backup is simply redone from the archive"],
  ];
  let cy = 4.0;
  chk.forEach(([k,v]) => {
    s.addShape(p.ShapeType.roundRect, { x:0.75, y:cy, w:5.6, h:0.78,
      rectRadius:0.07, fill:{color:TINT}, line:{type:"none"} });
    s.addText(k, { x:0.92, y:cy, w:1.35, h:0.78, fontFace:MONO, bold:true,
      fontSize:11.5, color:AMBER, margin:0, valign:"middle" });
    s.addText(v, { x:2.3, y:cy, w:3.9, h:0.78, fontFace:SANS, fontSize:10.5,
      color:INK, margin:0, valign:"middle", lineSpacingMultiple:1.0 });
    cy += 0.85;
  });

  s.addShape(p.ShapeType.roundRect, { x:6.9, y:1.6, w:5.95, h:4.95,
    rectRadius:0.1, fill:{color:LIGHT}, line:{color:GREEN, width:1.5} });
  circ(s, 7.15, 1.85, 0.5, GREEN, "16");
  s.addText("-A 16  \u2014  how much is on tape?", {
    x:7.8, y:1.85, w:4.85, h:0.5, fontFace:SANS, bold:true, fontSize:15,
    color:INK, margin:0, valign:"middle" });
  s.addText("Reports, per dataset and as a grand total, what is already backed up and what is still waiting. No files are touched \u2014 it is a pure read of GDEXDB.", {
    x:7.15, y:2.5, w:5.5, h:0.7, fontFace:SANS, fontSize:12.5, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  code(s, 7.15, 3.28, 5.5, 0.5, "dsquasar -a -A 16 -E", 13);
  reftable(s, 7.15, 3.95, 5.5, [1.75, 3.75], [
    ["Backed up", "file count and total size already on Quasar"],
    ["Ready",     "count and size of files waiting to be backed up"],
    ["Tar files",  "bfile records by status: N / T / A"],
    ["Per dataset", "one line each, with a summary row at the end"],
  ], ["Section", "What it shows"], GREEN, GREEN);
  s.addShape(p.ShapeType.roundRect, { x:7.15, y:5.72, w:5.5, h:0.62,
    rectRadius:0.08, fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Pair with -e or -E.  ", options:{ bold:true, color:DEEP } },
    { text:"Statistics are most useful mailed to the specialist.", options:{} },
  ], { x:7.35, y:5.72, w:5.1, h:0.62, fontFace:SANS, fontSize:11.5, color:INK,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================== 16 SCHEDULING / DELAYED MODE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Scheduling", TEAL);
  title(s, "-d PBS  \u2014  Running as a Batch Job");
  s.addText("Backing up the whole archive is far too big for a login session. -d turns the run into a delayed job: dsquasar writes a dscheck record and exits, and the dscheck daemon submits it to PBS when a slot is free.", {
    x:0.5, y:1.6, w:12.35, h:0.62, fontFace:SANS, fontSize:14.5, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.1 });
  code(s, 0.5, 2.3, 12.35, 0.5,
    "dsquasar -a -A 3 -e -b -d PBS            # the usual cron line", 13.5);

  const steps = [
    ["1", "Cron fires", "dsquasar -a -A 3 -e -b -d PBS runs on the login host, in background mode.", DEEP],
    ["2", "Size the job", "The detail file count is turned into a cpu count, and PBS qoptions are written:\nwalltime + select=1:ncpus=N:mem=Ngb.", TEAL],
    ["3", "dscheck record", "A record is inserted keyed on command + specialist + argv + workdir. The argv is left unchanged, so the next identical cron line is recognised as a duplicate and blocks.", GREEN],
    ["4", "Daemon submits", "dscheck picks the record up, submits to PBS, and dsquasar re-runs there \u2014 reading its process count back from the reserved ncpus.", AMBER],
    ["5", "Retry on failure", "TRYLMTS['dsquasar'] = 3: a failed run is resubmitted up to three times before the record is reported as failed.", DEEP],
  ];
  let sy = 3.0;
  steps.forEach(([num, hd, bd, col]) => {
    s.addShape(p.ShapeType.roundRect, { x:0.5, y:sy, w:12.35, h:0.6,
      rectRadius:0.07, fill:{color:TINT}, line:{type:"none"} });
    circ(s, 0.68, sy+0.08, 0.44, col, num, LIGHT, 13);
    s.addText(hd, { x:1.28, y:sy, w:1.85, h:0.6, fontFace:SANS, bold:true,
      fontSize:12.5, color:INK, margin:0, valign:"middle" });
    s.addText(bd, { x:3.15, y:sy, w:9.5, h:0.6, fontFace:SANS, fontSize:10.5,
      color:MUTE, margin:0, valign:"middle", lineSpacingMultiple:0.95 });
    sy += 0.68;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:sy+0.05, w:12.35, h:0.55,
    rectRadius:0.08, fill:{color:MID}, line:{type:"none"} });
  s.addText([
    { text:"-d takes optional HostName and TryCount.  ", options:{ bold:true, color:AMBERLT } },
    { text:"-d PBS is the normal form; a bare -d uses the configured default batch host.", options:{ color:"C3D7EE" } },
  ], { x:0.72, y:sy+0.05, w:11.9, h:0.55, fontFace:SANS, fontSize:11.5,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================== 17 MULTIPROCESSING SIZING
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Parallelism", AMBER);
  title(s, "How Many CPUs Does the Job Ask For?");
  s.addText("Tarring and uploading have very different shapes, so they are sized independently and the larger request wins \u2014 the phases run one after the other inside a single PBS allocation, sharing one ncpus.", {
    x:0.5, y:1.6, w:12.35, h:0.6, fontFace:SANS, fontSize:14.5, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.1 });

  // tarring card
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:2.35, w:6.05, h:2.5,
    rectRadius:0.1, fill:{color:LIGHT}, line:{color:DEEP, width:1.5} });
  s.addText("Tarring   (-A 2, -A 3)", { x:0.75, y:2.5, w:5.6, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  s.addText("One child per tar file \u2014 many small, cheap units.", {
    x:0.75, y:2.85, w:5.6, h:0.3, fontFace:SANS, fontSize:11.5, color:MUTE, margin:0 });
  code(s, 0.75, 3.2, 5.6, 0.75,
    "MPTLIMIT = 200   tar files per process\nMPTMAX   =  12   process ceiling", 11.5);
  s.addText("A small divisor with a high cap: lots of tar work should spread wide.", {
    x:0.75, y:4.05, w:5.6, h:0.6, fontFace:SANS, fontSize:11, color:INK,
    margin:0, valign:"top" });

  // uploading card
  s.addShape(p.ShapeType.roundRect, { x:6.9, y:2.35, w:5.95, h:2.5,
    rectRadius:0.1, fill:{color:LIGHT}, line:{color:GREEN, width:1.5} });
  s.addText("Uploading   (-A 4)", { x:7.15, y:2.5, w:5.5, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:GREEN, margin:0 });
  s.addText("One child per 90 GB batch \u2014 few large, network-bound units.", {
    x:7.15, y:2.85, w:5.5, h:0.3, fontFace:SANS, fontSize:11.5, color:MUTE, margin:0 });
  code(s, 7.15, 3.2, 5.5, 0.75,
    "MPBLIMIT = 900   tar files per process\nMPBMAX   =   4   process ceiling", 11.5);
  s.addText("A large divisor with a low cap: more Globus streams do not move more bytes.", {
    x:7.15, y:4.05, w:5.5, h:0.6, fontFace:SANS, fontSize:11, color:INK,
    margin:0, valign:"top" });

  s.addText("Why the two sets of numbers", { x:0.5, y:5.0, w:12.35, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:INK, margin:0 });
  s.addText("Both phases once shared a single limit. Tarring came out under-provisioned while uploading reserved cpus it could never use \u2014 cpus that PBS charges for and that other jobs then cannot have. Splitting the constants fixed both ends.", {
    x:0.5, y:5.35, w:12.35, h:0.55, fontFace:SANS, fontSize:12, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:6.0, w:12.35, h:0.6,
    rectRadius:0.08, fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"-m sets the count by hand.  ", options:{ bold:true, color:DEEP } },
    { text:"Interactively it is whatever you pass (default 1). In delay mode the count travels through the reserved PBS ncpus instead of the command line, so -m on a -d run is ignored.", options:{} },
  ], { x:0.72, y:6.0, w:11.9, h:0.6, fontFace:SANS, fontSize:11.5, color:INK,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================== 18 PBS WALLTIME GUARD
(() => {
  const s = p.addSlide(); s.background = { color: MID };
  kicker(s, "Walltime Guard", AMBERLT);
  s.addText("Stopping Cleanly Before PBS Kills the Job", {
    x:0.5, y:0.72, w:11, h:0.7, fontFace:SERIF, fontSize:32, bold:true,
    color:LIGHT, margin:0 });
  s.addText("The queue gives the job 24 hours. A tar file killed halfway through leaves a partial file on disk and a bfile record that claims work is in progress. So dsquasar watches the clock itself.", {
    x:0.5, y:1.62, w:12.35, h:0.6, fontFace:SANS, fontSize:14.5, color:"C3D7EE",
    margin:0, valign:"top", lineSpacingMultiple:1.1 });

  // timeline
  const tx = 0.9, tw = 11.5, ty = 2.75;
  s.addShape(p.ShapeType.roundRect, { x:tx, y:ty, w:tw, h:0.42, rectRadius:0.08,
    fill:{color:"0B2A63"}, line:{type:"none"} });
  s.addShape(p.ShapeType.roundRect, { x:tx, y:ty, w:tw*23/24, h:0.42, rectRadius:0.08,
    fill:{color:DEEP}, line:{type:"none"} });
  s.addText("work accepted   \u2014   0 h to 23 h", { x:tx, y:ty, w:tw*23/24, h:0.42,
    align:"center", valign:"middle", fontFace:SANS, bold:true, fontSize:12,
    color:LIGHT, margin:0 });
  s.addText("wind\ndown", { x:tx+tw*23/24, y:ty-0.02, w:tw/24, h:0.46, align:"center",
    valign:"middle", fontFace:SANS, bold:true, fontSize:8, color:AMBERLT, margin:0 });
  [["0 h", 0], ["23 h  MAXRUNTIME", 23/24], ["24 h  walltime", 1]].forEach(([lbl, f]) => {
    s.addText(lbl, { x:tx + tw*f - 0.9, y:ty+0.5, w:1.8, h:0.3, align:"center",
      fontFace:MONO, fontSize:10, color:"97999B", margin:0 });
  });

  const rules = [
    ["Before each unit", "The remaining time is compared against what the next tar file or transfer batch is expected to need."],
    ["Past 23 hours", "No new unit is started. Children already running are waited on so nothing is truncated."],
    ["Progress email", "The specialist is mailed what finished and what is still outstanding, so the next run is not a surprise."],
    ["Record kept open", "Untarred bfile records stay at 'N' and untransferred ones at 'T'. The next scheduled run simply resumes."],
  ];
  let ry = 3.55;
  rules.forEach(([k, v]) => {
    s.addShape(p.ShapeType.roundRect, { x:0.9, y:ry, w:11.5, h:0.68,
      rectRadius:0.07, fill:{color:"0B2A63"}, line:{color:"2C4E7D", width:1} });
    s.addText(k, { x:1.1, y:ry, w:2.5, h:0.68, fontFace:SANS, bold:true,
      fontSize:12.5, color:AMBERLT, margin:0, valign:"middle" });
    s.addText(v, { x:3.65, y:ry, w:8.55, h:0.68, fontFace:SANS, fontSize:11,
      color:"C3D7EE", margin:0, valign:"middle", lineSpacingMultiple:1.0 });
    ry += 0.75;
  });
  s.addText("The guard only runs inside a real batch job; a foreground run is left to the operator.", {
    x:0.9, y:ry+0.02, w:11.5, h:0.35, fontFace:SANS, italic:true, fontSize:11,
    color:"97999B", margin:0 });
  foot(s, true);
})();

// ============================================== 19 WORKER SLOTS
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Worker Slots", TEAL);
  title(s, "-W / -w  \u2014  When One Batch Job Hangs");
  s.addText("The -A 3 gather runs from cron several times a day, so a crashed job restarts quickly \u2014 dead locks are taken over and dscheck retries. The gap was a job that is alive but making no progress: it holds the duplicate key and every later cron line simply blocks.", {
    x:0.5, y:1.58, w:12.35, h:0.62, fontFace:SANS, fontSize:14, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.1 });

  s.addText("Stall test", { x:0.5, y:2.35, w:6.05, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  s.addText("A repeat submit inspects the running job. Both conditions must hold:", {
    x:0.5, y:2.7, w:6.05, h:0.32, fontFace:SANS, fontSize:11.5, color:MUTE, margin:0 });
  code(s, 0.5, 3.05, 6.05, 0.8,
    "runtime  >  WORKGRACE  (6 hours)\ndcount / fcount  <=  MINWDONE  (1 %)", 12);
  s.addText("Six hours in with under one percent of its recorded work done, the job is treated as stalled and an extra worker is submitted alongside it \u2014 up to -W MaxWorkers, default 2.", {
    x:0.5, y:4.0, w:6.05, h:0.65, fontFace:SANS, fontSize:11.5, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  code(s, 0.5, 4.75, 6.05, 0.5, "dsquasar -a -A 3 -b -d PBS -W 2", 12.5);
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.42, w:6.05, h:1.15,
    rectRadius:0.08, fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Pinned to a single worker when locking is off.  ", options:{ bold:true, color:DEEP } },
    { text:"Extra workers rely on dataset locks to stay off each other's files, so -l N and the transfer actions (-A 4, 6, 7) always run one worker.", options:{} },
  ], { x:0.7, y:5.42, w:5.65, h:1.15, fontFace:SANS, fontSize:11, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.05 });

  s.addText("Slot numbering and direction", { x:6.9, y:2.35, w:5.95, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  s.addText("-w N is appended to the extra worker's own command line. Since argv is the only differing key field, that gives it a dscheck record of its own instead of colliding with slot 1.", {
    x:6.9, y:2.7, w:5.95, h:0.62, fontFace:SANS, fontSize:11.5, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  reftable(s, 6.9, 3.42, 5.95, [1.35, 4.6], [
    ["slot 1", "plain argv \u2014 repeat cron still blocks as a duplicate"],
    ["odd",    "walks datasets in ascending dsid order"],
    ["even",   "walks datasets descending, from d999999"],
  ], ["Slot", "Behaviour"], DEEP);
  s.addText("A pair of workers therefore meets in the middle rather than fighting over the same dataset lock from the same end of the list.", {
    x:6.9, y:5.15, w:5.95, h:0.5, fontFace:SANS, fontSize:11.5, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  s.addShape(p.ShapeType.roundRect, { x:6.9, y:5.72, w:5.95, h:0.85,
    rectRadius:0.08, fill:{color:MID}, line:{type:"none"} });
  s.addText([
    { text:"Not a fix for one huge dataset.  ", options:{ bold:true, color:AMBERLT } },
    { text:"Locks are per dataset, so a second worker cannot help inside the dataset that is actually stuck.", options:{ color:"C3D7EE" } },
  ], { x:7.1, y:5.72, w:5.55, h:0.85, fontFace:SANS, fontSize:11,
       margin:0, valign:"middle", lineSpacingMultiple:1.05 });
  foot(s);
})();

// ============================================== 20 LOCKING & CONCURRENCY
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Locking", GREEN);
  title(s, "Keeping Two Runs Off the Same Files");
  s.addText("Every mutual exclusion in dsquasar is a dataset lock \u2014 a pid and host stamped on the dataset record, claimed atomically and taken over automatically when the owning process is dead. There is no lock at the tar-file level.", {
    x:0.5, y:1.58, w:12.35, h:0.6, fontFace:SANS, fontSize:14, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.1 });

  const cards = [
    ["Lock, then work", "The dataset is locked before its files are gathered or its tars are built, and unlocked as soon as that dataset is finished.", DEEP],
    ["Re-check under the lock", "Work is listed before the lock is held, so the list can be hours old. Each candidate is re-read under the lock \u2014 still status 'N'? \u2014 before a tar is spent on it.", TEAL],
    ["Multi-dataset tars", "One tar file may hold files from many datasets. All of them are locked before tarring and released once the tar is dispatched; if any is held elsewhere, the tar waits for a later run.", GREEN],
    ["Tar names are safe", "The predicted bid is only a guess. The insert uses AUTOID and the tar is renamed if the database hands back a different id.", AMBER],
  ];
  let cx = 0.5;
  cards.forEach(([hd, bd, col]) => {
    s.addShape(p.ShapeType.roundRect, { x:cx, y:2.3, w:2.99, h:2.55,
      rectRadius:0.1, fill:{color:LIGHT}, line:{color:col, width:1.5} });
    s.addShape(p.ShapeType.roundRect, { x:cx, y:2.3, w:2.99, h:0.1,
      rectRadius:0.03, fill:{color:col}, line:{type:"none"} });
    s.addText(hd, { x:cx+0.2, y:2.55, w:2.6, h:0.6, fontFace:SANS, bold:true,
      fontSize:13.5, color:col, margin:0, valign:"top" });
    s.addText(bd, { x:cx+0.2, y:3.2, w:2.6, h:1.5, fontFace:SANS, fontSize:11,
      color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.08 });
    cx += 3.11;
  });

  s.addText("Operator controls", { x:0.5, y:5.05, w:12.35, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:INK, margin:0 });
  reftable(s, 0.5, 5.42, 12.35, [1.5, 10.85], [
    ["-l Y | -l N", "Lock datasets before backing them up. Y is the default; N skips locking entirely and forces a single worker."],
    ["-u",          "Clean up backup locks left behind on the named datasets. Dataset IDs are required \u2014 not valid with -a."],
  ], ["Option", "Effect"], GREEN, GREEN);
  s.addText("Locks left by a killed process are reclaimed automatically; -u is for the cases where you want them gone now.", {
    x:0.5, y:6.7, w:12.35, h:0.3, fontFace:SANS, italic:true, fontSize:11,
    color:MUTE, margin:0 });
  foot(s);
})();

// ============================================== 21 EMAIL REPORTING
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Reporting", TEAL);
  title(s, "-e / -E  \u2014  Mail to the Specialist");
  s.addText("A batch run leaves no one watching the screen, so dsquasar mails the outcome to the dataset specialist. Two levels of detail.", {
    x:0.5, y:1.6, w:12.35, h:0.4, fontFace:SANS, fontSize:14.5, color:INK, margin:0 });
  const em = [
    ["-e", "Summary", "Totals only: how many files and bytes were backed up, and how much is still waiting.", DEEP],
    ["-E", "Detailed", "The same totals plus a per-dataset breakdown \u2014 which datasets moved, which are still behind.", GREEN],
  ];
  let ex = 0.5;
  em.forEach(([flag, hd, bd, col]) => {
    s.addShape(p.ShapeType.roundRect, { x:ex, y:2.15, w:6.05, h:1.5,
      rectRadius:0.1, fill:{color:TINT}, line:{color:col, width:1.5} });
    s.addText(flag, { x:ex+0.22, y:2.32, w:0.9, h:0.5, fontFace:MONO, bold:true,
      fontSize:22, color:col, margin:0, valign:"middle" });
    s.addText(hd, { x:ex+1.2, y:2.32, w:4.6, h:0.5, fontFace:SANS, bold:true,
      fontSize:16, color:INK, margin:0, valign:"middle" });
    s.addText(bd, { x:ex+0.22, y:2.9, w:5.6, h:0.65, fontFace:SANS, fontSize:11.5,
      color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    ex += 6.35;
  });

  s.addText("Typical summary mail", { x:0.5, y:3.9, w:12.35, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  code(s, 0.5, 4.28, 12.35, 1.85,
    "Subject: dsquasar backup statistics\n\n" +
    "Backed up : 148,392 files   612.4 GB   in 128 tar files\n" +
    "Ready     :  22,105 files    91.7 GB\n" +
    "Tar files : N = 18   T = 6   A = 128\n\n" +
    "d123456   12,004 files   58.2 GB  backed up      2,110 files ready\n" +
    "d999001    1,338 files    9.9 GB  backed up          0 files ready", 11.5);
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:6.28, w:12.35, h:0.6,
    rectRadius:0.08, fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"The walltime guard mails too.  ", options:{ bold:true, color:DEEP } },
    { text:"A batch job that stops at 23 hours sends a progress note listing what finished and what is left, so an incomplete run is visible rather than silent.", options:{} },
  ], { x:0.72, y:6.28, w:11.9, h:0.6, fontFace:SANS, fontSize:11.5, color:INK,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================== 22 KEY MODE OPTIONS
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Reference", AMBER);
  title(s, "Option Reference");
  reftable(s, 0.5, 1.55, 6.05, [1.35, 4.7], [
    ["-a",        "all datasets with a backup flag set"],
    ["-t ids",    "named datasets; SQL % wildcard allowed"],
    ["-B",        "Backup end point only"],
    ["-D",        "Backup and Drdata end points"],
    ["-A bits",   "actions: 1 2 3 4 6 7 8 16 (default 3)"],
    ["-c days",   "with -A 1, re-back up recently changed files"],
    ["-e / -E",   "mail summary / detailed statistics"],
  ], ["Selection & scope", "Meaning"], DEEP);

  reftable(s, 6.9, 1.55, 5.95, [1.35, 4.6], [
    ["-b",        "background mode, no screen output"],
    ["-d [h] [n]", "delay mode: submit as a PBS batch job"],
    ["-m N",      "concurrent child processes (default 1)"],
    ["-l Y | N",  "lock datasets before backing up (default Y)"],
    ["-n",        "show available file counts only, do nothing"],
    ["-W N",      "max concurrent batch workers (default 2)"],
    ["-w N",      "worker slot; set automatically, not by hand"],
    ["-u",        "clean up backup locks on the named datasets"],
    ["-h",        "display the help document"],
  ], ["Run control", "Meaning"], GREEN, GREEN);

  s.addText("Either -a or -t is required \u2014 without one, dsquasar prints its usage and exits. -B and -D are mutually exclusive; giving neither backs up to both end points.", {
    x:0.5, y:5.05, w:6.05, h:0.7, fontFace:SANS, fontSize:11.5, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  s.addText("Six lines that cover most days", { x:0.5, y:5.8, w:12.35, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:INK, margin:0 });
  code(s, 0.5, 6.15, 12.35, 0.85,
    "dsquasar -a -A 3 -e -b -d PBS     dsquasar -t d123456 -A 4\n" +
    "dsquasar -t d123456 -n            dsquasar -a -A 16 -e\n" +
    "dsquasar -t d123456 -A 1 -c 30    dsquasar -t d123456 -u", 11.5);
  foot(s);
})();

// ============================================== 23 ENVIRONMENT & FILES
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Environment", TEAL);
  title(s, "Where Things Live");
  s.addText("dsquasar always runs from the Quasar backup work directory \u2014 it changes there itself, so it can be launched from anywhere.", {
    x:0.5, y:1.58, w:12.35, h:0.4, fontFace:SANS, fontSize:14.5, color:INK, margin:0 });
  code(s, 0.5, 2.05, 12.35, 0.5, "$GDEXWORK/$COMMONUSER/quasar_backup", 14);

  reftable(s, 0.5, 2.75, 6.05, [2.3, 3.75], [
    ["<dsid>_<t>_<bid>.txt", "dsarch input list, one per type per tar"],
    ["<dsid>_sn..._fn....tar", "the built tar, deleted after transfer"],
    ["dsquasar.log",          "run log \u2014 first place to look"],
    ["dscheck record",        "batch state, retries and progress counts"],
  ], ["Work directory", "Contents"], DEEP);

  reftable(s, 6.9, 2.75, 5.95, [2.35, 3.6], [
    ["gdex-glade",         "source end point (archive on disk)"],
    ["gdex-quasar",        "Backup copy on tape"],
    ["gdex-quasar-drdata", "disaster-recovery copy"],
    ["/data/...",          "Web file staging path"],
    ["/decsdata/...",      "Saved file staging path"],
  ], ["Globus & paths", "Role"], GREEN, GREEN);

  s.addText("Related console commands", { x:0.5, y:5.05, w:12.35, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:INK, margin:0 });
  const cmds = [
    ["dsquasar", "back up GDEX archives onto the Quasar end points", DEEP],
    ["tacctar",  "build tar files for the TACC copy of the archive", TEAL],
    ["taccrec",  "recover files from the TACC copy", GREEN],
  ];
  let px = 0.5;
  cmds.forEach(([c, d, col]) => {
    s.addShape(p.ShapeType.roundRect, { x:px, y:5.45, w:4.05, h:1.0,
      rectRadius:0.08, fill:{color:TINT}, line:{color:col, width:1.2} });
    s.addText(c, { x:px+0.2, y:5.58, w:3.65, h:0.35, fontFace:MONO, bold:true,
      fontSize:14, color:col, margin:0 });
    s.addText(d, { x:px+0.2, y:5.92, w:3.65, h:0.5, fontFace:SANS, fontSize:11,
      color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.03 });
    px += 4.15;
  });
  s.addText("All three are installed by the rda_python_dsquasar package and share the rda_python_common configuration.", {
    x:0.5, y:6.58, w:12.35, h:0.3, fontFace:SANS, italic:true, fontSize:11,
    color:MUTE, margin:0 });
  foot(s);
})();

// ============================================== 24 TROUBLESHOOTING
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Troubleshooting", AMBER);
  title(s, "When Something Looks Wrong");
  const tr = [
    ["Nothing is gathered", "The dataset or group backflag is not set, or the group flag is 'N'. Confirm with  dsquasar -t <dsid> -n  before digging further.", DEEP],
    ["A repeat submit does nothing", "That is the duplicate check working \u2014 an identical command is already queued or running. Check the dscheck record before forcing anything.", TEAL],
    ["Records stuck at 'N'", "Tarring never reached them: the run hit the 23-hour guard, or the dataset was locked by another worker. Both resolve on the next run.", GREEN],
    ["Records stuck at 'T'", "The tar was built but not transferred. Run  -A 4  for that dataset. If the local tar is gone, the record is reset to 'N' and rebuilt.", AMBER],
    ["Sizes disagree with tape", "Run  -A 8  \u2014 mismatched records are reset to 'N' and redone from the archive on the next gather.", DEEP],
    ["Locks left on a dataset", "Locks held by a dead process are taken over automatically. To clear them now:  dsquasar -t <dsid> -u", TEAL],
  ];
  let y = 1.58;
  tr.forEach(([hd, bd, col]) => {
    s.addShape(p.ShapeType.roundRect, { x:0.5, y, w:12.35, h:0.72,
      rectRadius:0.07, fill:{color:TINT}, line:{type:"none"} });
    s.addShape(p.ShapeType.roundRect, { x:0.5, y, w:0.09, h:0.72,
      rectRadius:0.02, fill:{color:col}, line:{type:"none"} });
    s.addText(hd, { x:0.78, y, w:3.3, h:0.72, fontFace:SANS, bold:true,
      fontSize:12.5, color:col, margin:0, valign:"middle" });
    s.addText(bd, { x:4.15, y, w:8.5, h:0.72, fontFace:SANS, fontSize:11,
      color:INK, margin:0, valign:"middle", lineSpacingMultiple:1.0 });
    y += 0.8;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:y+0.02, w:12.35, h:0.55,
    rectRadius:0.08, fill:{color:MID}, line:{type:"none"} });
  s.addText([
    { text:"Statuses only ever move forward on success.  ", options:{ bold:true, color:AMBERLT } },
    { text:"Every backwards step \u2014 T\u2192N, A\u2192N \u2014 is deliberate, and just means the work is queued to be done again.", options:{ color:"C3D7EE" } },
  ], { x:0.72, y:y+0.02, w:11.9, h:0.55, fontFace:SANS, fontSize:11.5,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================== 25 RECOVERY
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Recovery", GREEN);
  title(s, "Getting Data Back Off Tape");
  s.addText("Backing up is only half the point. Because every tar is recorded in GDEXDB with the datasets and files it contains, recovery is a lookup rather than a search of the tape.", {
    x:0.5, y:1.6, w:12.35, h:0.5, fontFace:SANS, fontSize:14.5, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.1 });

  s.addShape(p.ShapeType.roundRect, { x:0.5, y:2.25, w:6.05, h:2.6,
    rectRadius:0.1, fill:{color:LIGHT}, line:{color:DEEP, width:1.5} });
  s.addText("Restore through dsarch", { x:0.75, y:2.42, w:5.6, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  s.addText("dsquasar writes the backup; dsarch reads it back. The -RQ action pulls a tar from Quasar and unpacks the requested files.", {
    x:0.75, y:2.78, w:5.6, h:0.6, fontFace:SANS, fontSize:11.5, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  code(s, 0.75, 3.45, 5.6, 0.55, "dsarch <dsid> -RQ ...", 13);
  s.addText("The bfile record supplies the tar name, the end point and the path, so the operator does not have to know where on tape the file sits.", {
    x:0.75, y:4.1, w:5.6, h:0.6, fontFace:SANS, fontSize:11, color:INK,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });

  s.addShape(p.ShapeType.roundRect, { x:6.9, y:2.25, w:5.95, h:2.6,
    rectRadius:0.1, fill:{color:LIGHT}, line:{color:GREEN, width:1.5} });
  s.addText("Two independent copies", { x:7.15, y:2.42, w:5.5, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:GREEN, margin:0 });
  s.addText("Every tar goes to both end points by default. Drdata is written first, so a run interrupted midway still leaves the disaster-recovery copy complete.", {
    x:7.15, y:2.78, w:5.5, h:0.6, fontFace:SANS, fontSize:11.5, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  reftable(s, 7.15, 3.45, 5.5, [2.0, 3.5], [
    ["gdex-quasar",        "primary Backup copy"],
    ["gdex-quasar-drdata", "disaster-recovery copy"],
  ], ["End point", "Copy"], GREEN, GREEN);

  s.addText("The TACC utilities", { x:0.5, y:5.05, w:12.35, h:0.35,
    fontFace:SANS, bold:true, fontSize:15, color:INK, margin:0 });
  s.addText("The same package ships tacctar and taccrec for the separate TACC copy of the archive: tacctar builds the tar files, taccrec recovers from them. They follow the same tar-then-transfer shape but talk to the TACC servers rather than the Quasar Globus end points.", {
    x:0.5, y:5.42, w:12.35, h:0.7, fontFace:SANS, fontSize:12, color:MUTE,
    margin:0, valign:"top", lineSpacingMultiple:1.05 });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:6.2, w:12.35, h:0.6,
    rectRadius:0.08, fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Verify before you need it.  ", options:{ bold:true, color:DEEP } },
    { text:"A scheduled  -A 8  run is what turns \u201cwe have backups\u201d into \u201cwe have backups that match the archive\u201d.", options:{} },
  ], { x:0.72, y:6.2, w:11.9, h:0.6, fontFace:SANS, fontSize:11.5, color:INK,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================== 26 CLOSING
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Wrap Up", TEAL);
  title(s, "Six Things Worth Remembering");
  const pts = [
    ["Flags decide, not you", "Backup flags on the dataset and group records choose what is gathered. -t narrows the run; it does not override a flag.", DEEP],
    ["-A 3 is the working default", "Create the input lists and build the tars in one pass. -A 4 ships them. -A 7 does both.", TEAL],
    ["Status is the whole story", "N listed, T tarred, A archived. Backwards moves are deliberate and simply requeue the work.", GREEN],
    ["Tar size is the economics", "5 GB target tars batched into 90 GB transfers is what makes tape and Globus efficient.", AMBER],
    ["The dataset lock is the only lock", "Everything about concurrency \u2014 workers, re-checks, multi-dataset tars \u2014 is built on it.", DEEP],
    ["Nothing is lost by stopping", "The 23-hour guard, the retry limit and the resumable statuses mean an interrupted run just continues later.", TEAL],
  ];
  let px = 0.5, py = 1.6;
  pts.forEach(([hd, bd, col], i) => {
    s.addShape(p.ShapeType.roundRect, { x:px, y:py, w:6.05, h:1.5,
      rectRadius:0.1, fill:{color:LIGHT}, line:{color:LINE, width:1} });
    circ(s, px+0.22, py+0.24, 0.46, col, String(i+1), LIGHT, 14);
    s.addText(hd, { x:px+0.82, y:py+0.18, w:5.0, h:0.42, fontFace:SANS, bold:true,
      fontSize:13.5, color:col, margin:0, valign:"middle" });
    s.addText(bd, { x:px+0.82, y:py+0.62, w:5.0, h:0.7, fontFace:SANS, fontSize:11,
      color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    if (i % 2 === 0) { px += 6.35; } else { px = 0.5; py += 1.65; }
  });
  foot(s);
})();

// ============================================== 27 QUESTIONS
(() => {
  const s = p.addSlide(); s.background = { color: MID };
  s.addImage({ path: LOGO_WHITE, x:0.7, y:0.6, w:2.8, h:0.77 });
  s.addImage({ path: GDEX_WHITE, x:W-GDEX_W-0.6, y:0.7, w:GDEX_W, h:GDEX_H });
  s.addText("Questions?", { x:0.7, y:2.5, w:9, h:1.3, fontFace:SERIF,
    fontSize:66, bold:true, color:LIGHT, margin:0 });
  s.addText("dsquasar  \u2014  Backing Up the GDEX Archive onto Quasar", {
    x:0.72, y:3.85, w:11.5, h:0.5, fontFace:SANS, fontSize:20,
    color:"C3D7EE", margin:0 });
  s.addText("dsquasar -h        for the full help document", {
    x:0.72, y:4.6, w:11.5, h:0.4, fontFace:MONO, fontSize:14,
    color:AMBERLT, margin:0 });
  s.addText([
    { text:"Zaihua Ji", options:{ bold:true } },
    { text:"   zji@ucar.edu   \u2022   github.com/NCAR/rda-python-dsquasar", options:{} },
  ], { x:0.72, y:5.6, w:11.5, h:0.4, fontFace:SANS, fontSize:14,
       color:"C3D7EE", margin:0 });
})();

p.writeFile({ fileName: "dsquasar_guide.pptx" }).then(f => console.log("wrote", f));
