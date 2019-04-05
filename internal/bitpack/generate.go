package bitpack

// Currently bitpacking is only used for definition levels, so only
// width 1 is used. The reason max is set to 3 is so a test could be
// written based on the example in Apache's bitpack documentation.

//go:generate bitpackgen -package bitpack -maxwidth 3
