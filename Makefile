## make file for libstash.

all: libstash.so.1.0.1

ARGS=-g -Wall
OBJS=libstash.o

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
	

man-pages: libstash.3
	cp libstash.3 $(MANPATH)/man3/
	@gzip libstash.3
	@echo "Man-pages Install complete."


clean:
	@-[ -e libstash.o ] && rm libstash.o
	@-[ -e libstash.so* ] && rm libstash.so*
