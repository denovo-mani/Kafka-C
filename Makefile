BINS=producer

SRCS=common.c json.c
LIBS=-lrdkafka -lm

all: $(BINS)

producer: producer.c $(SRCS)
	$(CC) $(CPPFLAGS) $(LDFLAGS) $^ -o $@ $(LIBS)

clean:
	rm -f *.o $(BINS)
