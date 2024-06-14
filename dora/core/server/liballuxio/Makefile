ALLUXIO_TARGET_LIB := liballuxio.so
OUTPUT_DIR ?= ./build
SRC_DIR ?= ./src

# Notice the change from *.cpp to *.o in the patsubst function
LIBALLUXIO_OBJS = $(addprefix $(OUTPUT_DIR)/, $(notdir $(patsubst %.cpp,%.o,$(wildcard $(SRC_DIR)/*.cpp))))

${ALLUXIO_TARGET_LIB}: ${LIBALLUXIO_OBJS}
	${CXX} ${COMMON_FLAGS} -shared -o ${OUTPUT_DIR}/${ALLUXIO_TARGET_LIB} ${LIBALLUXIO_OBJS}

$(OUTPUT_DIR)/%.o: $(SRC_DIR)/%.cpp
	mkdir -p $(OUTPUT_DIR)
	${CXX} ${COMMON_FLAGS} -c $< -o $@

all: ${ALLUXIO_TARGET_LIB}

clean:
	rm -f ${OUTPUT_DIR}/*.o ${OUTPUT_DIR}/${ALLUXIO_TARGET_LIB}

