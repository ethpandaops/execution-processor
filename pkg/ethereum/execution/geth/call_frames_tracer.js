{
  n: 0,
  frames: [],
  open: [],
  step: function(log, db) { this.n++; },
  enter: function(f) {
    var t = f.getType();
    if (t !== "CALL" && t !== "CALLCODE" && t !== "DELEGATECALL" && t !== "STATICCALL" && t !== "CREATE" && t !== "CREATE2") {
      this.open.push(-1);
      return;
    }
    this.open.push(this.frames.length);
    this.frames.push({i: this.n - 1, t: t, o: "", e: ""});
  },
  exit: function(r) {
    var i = this.open.pop();
    if (i < 0) {
      return;
    }
    var out = r.getOutput();
    var err = r.getError();
    this.frames[i].o = out ? toHex(out) : "";
    this.frames[i].e = err ? err : "";
  },
  fault: function(log, db) {},
  result: function(ctx, db) {
    return {frames: this.frames, gasUsed: ctx.gasUsed, error: ctx.error ? ctx.error : "", output: ctx.output ? toHex(ctx.output) : ""};
  }
}
