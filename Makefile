.PHONY: build install uninstall clean

NAME := search
TARGET_DIR := build/libs
RELEASE_BINARY := $(TARGET_DIR)/s-1.0-SNAPSHOT-all.jar
DEST_DIR := $(HOME)/bin
DEST_BINARY := $(DEST_DIR)/$(NAME).jar

build:
	./gradlew fatJar

install: build
	@mkdir -p "$(DEST_DIR)"
	@rm -f "$(DEST_BINARY)"
	@cp "$(RELEASE_BINARY)" "$(DEST_BINARY)"
	@chmod +x "$(DEST_BINARY)"
	@echo "Installed $(DEST_BINARY)"

uninstall:
	@rm -f "$(DEST_BINARY)"
	@echo "Uninstalled $(DEST_BINARY)"

clean:
	./gradlew clean

run:
	java --add-exports=java.desktop/com.apple.eawt=ALL-UNNAMED -jar $(DEST_BINARY)

test:
	./gradlew test