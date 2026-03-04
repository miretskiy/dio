module github.com/miretskiy/dio

go 1.25.0

require (
	github.com/HdrHistogram/hdrhistogram-go v1.2.0
	github.com/pawelgaczynski/giouring v0.0.0-20230826085535-69588b89acb9
	golang.org/x/sys v0.41.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/pawelgaczynski/giouring => ../giouring
