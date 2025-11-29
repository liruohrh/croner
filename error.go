package croner

import "fmt"

func wrapErrs(errs ...error) error {
	var ferr error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if ferr == nil {
			ferr = err
			continue
		}
		ferr = fmt.Errorf("%w: %w", err, ferr)
	}
	return ferr
}
