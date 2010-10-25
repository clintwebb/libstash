## make file for libstash.

all: libstash.so.1.0.1

ARGS=-g -Wall
OBJS=libstash.o
MANPATH=/usr/local/man

libstash.o: libstash.c stash.h 
	gcc -c -fPIC libstash.c  -o $@ $(ARGS)


libstash.a: $(OBJS)
	@>$@
	@rm $@
	ar -r $@
	ar -r $@ $^

libstash.so.1.0.1: $(OBJS)
	gcc -shared -Wl,-soname,libstash.so.1 -o libstash.so.1.0.1 $(OBJS)
	

install: libstash.so.1.0.1 stash.h
	@-test -e /usr/include/stash.h && rm /usr/include/stash.h
	cp stash.h /usr/include/
	cp libstash.so.1.0.1 /usr/lib/
	@-test -e /usr/lib/libstash.so && rm /usr/lib/libstash.so
	ln -s /usr/lib/libstash.so.1.0.1 /usr/lib/libstash.so
	ldconfig
	@echo "Install complete."


uninstall: /usr/include/stash.h /usr/lib/libstash.so.1.0.1
	rm /usr/include/stash.h
	rm /usr/lib/libstash.so.1.0.1
	rm /usr/lib/libstash.so.1
	rm /usr/lib/libstash.so

libstash.3.gz: libstash.3
	gzip -c $^ > $@

stash_t.3.gz: stash_t.3
	gzip -c $^ > $@

stash_init.3.gz: stash_init.3
	gzip -c $^ > $@

stash_free.3.gz: stash_free.3
	gzip -c $^ > $@

stash_shutdown.3.gz: stash_shutdown.3
	gzip -c $^ > $@

man-pages: libstash.3.gz stash_t.3.gz stash_init.3.gz stash_free.3.gz stash_shutdown.3.gz
	cp libstash.3.gz $(MANPATH)/man3/
	cp stash_t.3.gz $(MANPATH)/man3/
	cp stash_init.3.gz $(MANPATH)/man3/
	cp stash_free.3.gz $(MANPATH)/man3/
	cp stash_shutdown.3.gz $(MANPATH)/man3/
	@echo "Man-pages Install complete."


clean:
	@-[ -e libstash.o ] && rm libstash.o
	@-[ -e libstash.so* ] && rm libstash.so*
	@-rm *.3.gz
