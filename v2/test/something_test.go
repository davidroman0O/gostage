package test

import (
	v2 "github.com/davidroman0O/gostage/v2"
)

func ExampleSomething() {
	v2.NewAction("example", "An example action", func(ctx v2.Context) error {
		return nil
	})
}
