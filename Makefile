.PHONY: all
all: test

test:
	go get -v github.com/jstemmer/go-junit-report
	go build -o go-junit-report github.com/jstemmer/go-junit-report
	go get -v
	go test -v -run=Test_Unit 2>&1 | ./go-junit-report > report.xml

.PHONY: install
install:
	go install

.PHONY: clean 
clean:
	rm -fv *linux.amd64
	rm -fv *.darwin.amd64
	rm -fv *.windows.amd64
	find . -name "*~" | xargs rm -fv
	rm -fv go-junit-report report.xml

