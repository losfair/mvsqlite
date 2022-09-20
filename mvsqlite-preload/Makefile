CFLAGS += -O2 -fPIC

build-preload: preload.o shim.o
	$(CC) -O2 -shared \
		-Wl,-z,noexecstack \
		-o libmvsqlite_preload.so \
		preload.o shim.o \
		-L../target/release -lmvsqlite -lsqlite3 -lssl -lcrypto -lpthread -ldl -lm

build-lib: shim.o
	ar rcs libmvsqlite.a shim.o

clean:
	rm -f libmvsqlite_preload.so preload.o shim.o

.PHONY: clean
