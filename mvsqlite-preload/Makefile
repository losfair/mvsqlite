CFLAGS += -O2 -fPIC

build: preload.o
	$(CC) -O2 -shared -o libmvsqlite_preload.so preload.o -L../target/release -lmvsqlite -lsqlite3 -lssl -lcrypto -lpthread -ldl -lm

clean:
	rm -f libmvsqlite_preload.so preload.o

.PHONY: clean
