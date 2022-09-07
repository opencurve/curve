#if defined(HAVE_ASM_WEAK_DIRECTIVE) || defined(HAVE_ASM_WEAKEXT_DIRECTIVE)
#define HAVE_WEAK_SYMBOLS
#endif

#define STRINGOF(x) #x

/*
 * Define alias, asym, as a strong alias for symbol, sym.
 */
#define sysio_sym_strong_alias(sym, asym) \
  extern __typeof(sym) asym __attribute__((alias(STRINGOF(sym))));

#ifdef HAVE_WEAK_SYMBOLS

/*
 * Define alias, asym, as a strong alias for symbol, sym.
 */
#define sysio_sym_weak_alias(sym, asym) \
  extern __typeof(sym) asym __attribute__((weak, alias(STRINGOF(sym))));
#else /* !defined(HAVE_ASM_WEAK_DIRECTIVE) */

/*
 * Weak symbols not supported. Make it a strong alias then.
 */
#define sysio_sym_weak_alias(sym, asym) sysio_sym_strong_alias(sym, asym)
#endif
