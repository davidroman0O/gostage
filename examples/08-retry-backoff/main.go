// Example 08: Retry with Exponential Backoff
//
// A task that fails 3 times before succeeding. Uses WithRetry(5) to
// allow up to 5 retries and WithRetryStrategy with ExponentialBackoffWithJitter
// to add increasing delays between attempts. Each attempt is logged with
// timing so you can see the backoff behavior.
package main

import (
	"fmt"
	"log"
	"context"
	"time"

	gs "github.com/davidroman0O/gostage"
)

var attempt int
var startTime time.Time

func main() {
	gs.ResetTaskRegistry()
	startTime = time.Now()

	gs.Task("flaky-service", func(ctx *gs.Ctx) error {
		attempt++
		elapsed := time.Since(startTime).Truncate(time.Millisecond)
		if attempt <= 3 {
			fmt.Printf("[attempt %d] +%s  FAILED (simulated error)\n", attempt, elapsed)
			return fmt.Errorf("service unavailable (attempt %d)", attempt)
		}
		fmt.Printf("[attempt %d] +%s  SUCCESS\n", attempt, elapsed)
		gs.Set(ctx, "result", "data from service")
		return nil
	},
		gs.WithRetry(5),
		gs.WithRetryStrategy(gs.ExponentialBackoffWithJitter(
			50*time.Millisecond,  // base delay
			2*time.Second,        // max delay
		)),
	)

	wf, err := gs.NewWorkflow("retry-demo").
		Step("flaky-service").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}

	val, _ := gs.ResultGet[string](result, "result")
	fmt.Printf("\nFinal status: %s\n", result.Status)
	fmt.Printf("Result value: %s\n", val)
}
