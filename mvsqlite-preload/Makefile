CFLAGS += -O2 -fPIC

build-preload: preload.o shim.o
	$(CC) -O2 -shared -o libmvsqlite_preload.so preload.o shim.o -L../target/release -lmvsqlite -lsqlite3 -lssl -lcrypto -lpthread -ldl -lm

build-lib: shim.o
	$(CC) -O2 -shared -o libmvsqlite.so shim.o -L../target/release -lmvsqlite -lsqlite3 -lssl -lcrypto -lpthread -ldl -lm

clean:
	rm -f libmvsqlite_preload.so preload.o shim.o

.PHONY: clean
