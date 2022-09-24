CFLAGS += -O2 -fPIC

build-preload: preload.o shim.o
	$(CC) -O2 -shared \
		-Wl,-z,noexecstack \
		-o libmvsqlite_preload.so \
		preload.o shim.o \
		-L../target/release -lmvsqlite -lsqlite3 -lssl -lcrypto -lpthread -ldl -lm

build-lib: shim.o
	ar rcs libmvsqlite.a shim.o

build-patched-sqlite3:
	$(CC) -O2 -fPIC -shared -o libsqlite3.so \
		-Wl,-z,noexecstack \
		-DSQLITE_OMIT_UTF16 \
		-DSQLITE_ENABLE_FTS3 \
		-DSQLITE_ENABLE_FTS4 \
		-DMV_STATIC_PATCH \
		./sqlite3.c ./preload.c ./shim.c \
			-L../target/release -lmvsqlite -lpthread -ldl -lm

clean:
	rm -f libmvsqlite_preload.so preload.o shim.o

.PHONY: clean
