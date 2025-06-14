package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// ConfigProcessorAction reads configuration from store and sets up processing parameters
type ConfigProcessorAction struct {
	gostage.BaseAction
}

func (a *ConfigProcessorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("ConfigProcessor: Reading configuration from store")

	// Read app configuration
	if appConfig, err := store.Get[map[string]interface{}](ctx.Workflow.Store, "app_config"); err == nil {
		ctx.Logger.Info("Found app config: %+v", appConfig)

		// Extract and set processing parameters
		if env, ok := appConfig["environment"].(string); ok {
			ctx.Workflow.Store.Put("current_environment", env)

			// Set environment-specific settings
			switch env {
			case "development":
				ctx.Workflow.Store.Put("debug_mode", true)
				ctx.Workflow.Store.Put("log_level", "debug")
				ctx.Workflow.Store.Put("batch_size", 10)
			case "production":
				ctx.Workflow.Store.Put("debug_mode", false)
				ctx.Workflow.Store.Put("log_level", "info")
				ctx.Workflow.Store.Put("batch_size", 100)
			case "testing":
				ctx.Workflow.Store.Put("debug_mode", true)
				ctx.Workflow.Store.Put("log_level", "warn")
				ctx.Workflow.Store.Put("batch_size", 5)
			}
		}

		// Process database configuration
		if dbConfig, ok := appConfig["database"].(map[string]interface{}); ok {
			ctx.Workflow.Store.Put("db_host", dbConfig["host"])
			ctx.Workflow.Store.Put("db_port", dbConfig["port"])
			ctx.Workflow.Store.Put("db_name", dbConfig["name"])
			ctx.Logger.Info("Database config processed: %s:%v/%s", dbConfig["host"], dbConfig["port"], dbConfig["name"])
		}

		// Process feature flags
		if features, ok := appConfig["features"].(map[string]interface{}); ok {
			enabledFeatures := make([]string, 0)
			for feature, enabled := range features {
				if enabled.(bool) {
					enabledFeatures = append(enabledFeatures, feature)
				}
			}
			ctx.Workflow.Store.Put("enabled_features", enabledFeatures)
			ctx.Logger.Info("Enabled features: %v", enabledFeatures)
		}
	} else {
		ctx.Logger.Error("No app configuration found in store: %v", err)
		return fmt.Errorf("missing app configuration: %w", err)
	}

	// Add processing metadata
	ctx.Workflow.Store.Put("config_processed_at", time.Now().Format("2006-01-02 15:04:05"))
	ctx.Workflow.Store.Put("config_processor_version", "1.2.0")

	return nil
}

// DataTransformerAction processes input data based on configuration
type DataTransformerAction struct {
	gostage.BaseAction
}

func (a *DataTransformerAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("DataTransformer: Processing input data")

	// Get processing configuration
	batchSize, _ := store.GetOrDefault[int](ctx.Workflow.Store, "batch_size", 10)
	debugMode, _ := store.GetOrDefault[bool](ctx.Workflow.Store, "debug_mode", false)

	ctx.Logger.Info("Processing with batch_size=%d, debug_mode=%v", batchSize, debugMode)

	// Process input data
	if inputData, err := store.Get[[]interface{}](ctx.Workflow.Store, "input_data"); err == nil {
		ctx.Logger.Info("Processing %d input records", len(inputData))

		transformedData := make([]map[string]interface{}, 0, len(inputData))
		var totalProcessed int
		var errors []string

		// Process in batches
		for i := 0; i < len(inputData); i += batchSize {
			end := i + batchSize
			if end > len(inputData) {
				end = len(inputData)
			}

			batchData := inputData[i:end]
			ctx.Logger.Debug("Processing batch %d-%d (%d records)", i+1, end, len(batchData))

			// Transform each record in the batch
			for j, record := range batchData {
				if recordMap, ok := record.(map[string]interface{}); ok {
					transformedRecord := map[string]interface{}{
						"id":                fmt.Sprintf("record_%d", i+j+1),
						"original":          recordMap,
						"processed_at":      time.Now().Format("2006-01-02 15:04:05"),
						"batch_number":      (i / batchSize) + 1,
						"position_in_batch": j + 1,
					}

					// Apply transformations based on record type
					if recordType, ok := recordMap["type"].(string); ok {
						switch recordType {
						case "user":
							if name, ok := recordMap["name"].(string); ok {
								transformedRecord["display_name"] = fmt.Sprintf("User: %s", name)
								transformedRecord["category"] = "users"
							}
						case "product":
							if name, ok := recordMap["name"].(string); ok {
								transformedRecord["display_name"] = fmt.Sprintf("Product: %s", name)
								transformedRecord["category"] = "catalog"
							}
							if price, ok := recordMap["price"].(float64); ok {
								transformedRecord["price_category"] = categorizePrice(price)
								transformedRecord["tax_amount"] = price * 0.1 // 10% tax
							}
						case "order":
							if orderID, ok := recordMap["order_id"].(string); ok {
								transformedRecord["display_name"] = fmt.Sprintf("Order: %s", orderID)
								transformedRecord["category"] = "transactions"
							}
							if amount, ok := recordMap["amount"].(float64); ok {
								transformedRecord["amount_category"] = categorizeAmount(amount)
							}
						default:
							transformedRecord["display_name"] = "Unknown Type"
							transformedRecord["category"] = "uncategorized"
						}
					}

					// Add debug information if enabled
					if debugMode {
						transformedRecord["debug_info"] = map[string]interface{}{
							"processor":     "DataTransformerAction",
							"batch_size":    batchSize,
							"original_size": len(fmt.Sprintf("%+v", recordMap)),
						}
					}

					transformedData = append(transformedData, transformedRecord)
					totalProcessed++
				} else {
					errorMsg := fmt.Sprintf("Invalid record format at position %d", i+j)
					errors = append(errors, errorMsg)
					ctx.Logger.Warn(errorMsg)
				}
			}

			// Simulate processing delay for larger batches
			if batchSize > 20 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		// Store transformation results
		ctx.Workflow.Store.Put("transformed_data", transformedData)
		ctx.Workflow.Store.Put("transformation_stats", map[string]interface{}{
			"total_input":     len(inputData),
			"total_processed": totalProcessed,
			"total_errors":    len(errors),
			"batch_size_used": batchSize,
			"debug_mode":      debugMode,
			"processing_time": time.Now().Format("2006-01-02 15:04:05"),
		})

		if len(errors) > 0 {
			ctx.Workflow.Store.Put("transformation_errors", errors)
		}

		ctx.Logger.Info("Transformation complete: %d processed, %d errors", totalProcessed, len(errors))
	} else {
		ctx.Logger.Error("No input data found: %v", err)
		return fmt.Errorf("missing input data: %w", err)
	}

	return nil
}

// ReportGeneratorAction creates a summary report of the processing
type ReportGeneratorAction struct {
	gostage.BaseAction
}

func (a *ReportGeneratorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("ReportGenerator: Creating processing report")

	report := map[string]interface{}{
		"generated_at": time.Now().Format("2006-01-02 15:04:05"),
		"workflow_id":  ctx.Workflow.ID,
		"stage_id":     ctx.Stage.ID,
	}

	// Collect configuration information
	if env, err := store.Get[string](ctx.Workflow.Store, "current_environment"); err == nil {
		report["environment"] = env
	}

	if features, err := store.Get[[]string](ctx.Workflow.Store, "enabled_features"); err == nil {
		report["enabled_features"] = features
	}

	// Collect processing statistics
	if stats, err := store.Get[map[string]interface{}](ctx.Workflow.Store, "transformation_stats"); err == nil {
		report["processing_stats"] = stats
	}

	// Analyze transformed data
	if transformedData, err := store.Get[[]map[string]interface{}](ctx.Workflow.Store, "transformed_data"); err == nil {
		categoryCount := make(map[string]int)
		for _, record := range transformedData {
			if category, ok := record["category"].(string); ok {
				categoryCount[category]++
			}
		}

		report["data_analysis"] = map[string]interface{}{
			"total_records":      len(transformedData),
			"category_breakdown": categoryCount,
		}

		// Calculate some metrics
		userCount := categoryCount["users"]
		productCount := categoryCount["catalog"]
		orderCount := categoryCount["transactions"]

		report["business_metrics"] = map[string]interface{}{
			"user_records":    userCount,
			"product_records": productCount,
			"order_records":   orderCount,
			"data_quality":    calculateDataQuality(transformedData),
		}
	}

	// Check for errors
	if errors, err := store.Get[[]string](ctx.Workflow.Store, "transformation_errors"); err == nil {
		report["errors"] = errors
		report["has_errors"] = len(errors) > 0
	} else {
		report["has_errors"] = false
	}

	// Add processing recommendations
	report["recommendations"] = generateRecommendations(report)

	// Store the final report
	ctx.Workflow.Store.Put("final_report", report)

	ctx.Logger.Info("Report generated with %d sections", len(report))
	return nil
}

// Helper functions
func categorizePrice(price float64) string {
	switch {
	case price < 10:
		return "budget"
	case price < 50:
		return "standard"
	case price < 200:
		return "premium"
	default:
		return "luxury"
	}
}

func categorizeAmount(amount float64) string {
	switch {
	case amount < 25:
		return "small"
	case amount < 100:
		return "medium"
	case amount < 500:
		return "large"
	default:
		return "enterprise"
	}
}

func calculateDataQuality(data []map[string]interface{}) float64 {
	if len(data) == 0 {
		return 0.0
	}

	validRecords := 0
	for _, record := range data {
		// Check if record has required fields
		if _, hasID := record["id"]; hasID {
			if _, hasCategory := record["category"]; hasCategory {
				if _, hasDisplayName := record["display_name"]; hasDisplayName {
					validRecords++
				}
			}
		}
	}

	return float64(validRecords) / float64(len(data)) * 100
}

func generateRecommendations(report map[string]interface{}) []string {
	recommendations := make([]string, 0)

	// Check data quality
	if businessMetrics, ok := report["business_metrics"].(map[string]interface{}); ok {
		if quality, ok := businessMetrics["data_quality"].(float64); ok {
			if quality < 90 {
				recommendations = append(recommendations, "Consider improving data validation - quality is below 90%")
			}
		}
	}

	// Check for errors
	if hasErrors, ok := report["has_errors"].(bool); ok && hasErrors {
		recommendations = append(recommendations, "Review and fix data transformation errors")
	}

	// Check processing efficiency
	if stats, ok := report["processing_stats"].(map[string]interface{}); ok {
		if batchSize, ok := stats["batch_size_used"].(int); ok {
			if totalInput, ok := stats["total_input"].(int); ok {
				if totalInput > 1000 && batchSize < 50 {
					recommendations = append(recommendations, "Consider increasing batch size for better performance with large datasets")
				}
			}
		}
	}

	if len(recommendations) == 0 {
		recommendations = append(recommendations, "No issues detected - processing completed successfully")
	}

	return recommendations
}

func main() {
	fmt.Println("🚀 Store Workflow Example - Demonstrating InitialStore and FinalStore")
	fmt.Println()

	// Create workflow with multiple stages
	workflow := gostage.NewWorkflow("data-processing", "Data Processing Workflow", "Demonstrates store passing in normal workflows")

	// Stage 1: Configuration Processing
	configStage := gostage.NewStage("config-stage", "Configuration Processing", "Process application configuration")
	configStage.AddAction(&ConfigProcessorAction{
		BaseAction: gostage.NewBaseAction("config-processor", "Processes app configuration from store"),
	})
	workflow.AddStage(configStage)

	// Stage 2: Data Transformation
	transformStage := gostage.NewStage("transform-stage", "Data Transformation", "Transform input data based on configuration")
	transformStage.AddAction(&DataTransformerAction{
		BaseAction: gostage.NewBaseAction("data-transformer", "Transforms input data"),
	})
	workflow.AddStage(transformStage)

	// Stage 3: Report Generation
	reportStage := gostage.NewStage("report-stage", "Report Generation", "Generate processing report")
	reportStage.AddAction(&ReportGeneratorAction{
		BaseAction: gostage.NewBaseAction("report-generator", "Generates processing report"),
	})
	workflow.AddStage(reportStage)

	// Prepare initial store data
	fmt.Println("📦 Setting up initial store data...")

	initialStore := map[string]interface{}{
		"app_config": map[string]interface{}{
			"environment": "development",
			"version":     "2.1.0",
			"database": map[string]interface{}{
				"host": "localhost",
				"port": 5432,
				"name": "myapp_dev",
			},
			"features": map[string]interface{}{
				"analytics":     true,
				"caching":       true,
				"notifications": false,
				"beta_features": true,
			},
		},
		"input_data": []interface{}{
			map[string]interface{}{"type": "user", "name": "Alice Johnson", "email": "alice@example.com"},
			map[string]interface{}{"type": "user", "name": "Bob Smith", "email": "bob@example.com"},
			map[string]interface{}{"type": "product", "name": "Laptop Pro", "price": 1299.99, "category": "electronics"},
			map[string]interface{}{"type": "product", "name": "Coffee Mug", "price": 15.50, "category": "home"},
			map[string]interface{}{"type": "order", "order_id": "ORD-001", "amount": 1315.49, "status": "completed"},
			map[string]interface{}{"type": "order", "order_id": "ORD-002", "amount": 45.99, "status": "pending"},
			map[string]interface{}{"type": "product", "name": "Smartphone", "price": 799.99, "category": "electronics"},
			map[string]interface{}{"type": "user", "name": "Carol Davis", "email": "carol@example.com"},
		},
		"metadata": map[string]interface{}{
			"source":       "example_generator",
			"created_at":   time.Now().Format("2006-01-02 15:04:05"),
			"data_version": "1.0",
			"description":  "Sample data for store workflow demonstration",
		},
	}

	// Create run options with initial store
	options := gostage.RunOptions{
		Logger:       gostage.NewDefaultLogger(),
		Context:      context.Background(),
		IgnoreErrors: false,
		InitialStore: initialStore,
	}

	fmt.Printf("📋 Starting workflow with %d initial store keys...\n", len(initialStore))
	fmt.Println()

	// Execute workflow with initial store
	startTime := time.Now()
	result := gostage.RunWorkflow(workflow, options)
	duration := time.Since(startTime)

	// Display results
	fmt.Printf("⏱️  Workflow execution completed in %v\n", duration)
	fmt.Printf("✅ Success: %v\n", result.Success)
	if result.Error != nil {
		fmt.Printf("❌ Error: %v\n", result.Error)
		return
	}
	fmt.Println()

	// Display comprehensive results
	fmt.Println("📊 === EXECUTION SUMMARY ===")
	fmt.Printf("Workflow ID: %s\n", result.WorkflowID)
	fmt.Printf("Execution Time: %v\n", result.ExecutionTime)
	fmt.Printf("Initial store keys: %d\n", len(initialStore))
	fmt.Printf("Final store keys: %d\n", len(result.FinalStore))
	fmt.Println()

	// Show store data transformation
	fmt.Println("📦 === STORE DATA TRANSFORMATION ===")

	fmt.Println("📤 Initial Store:")
	for key, value := range initialStore {
		fmt.Printf("  %s: %s\n", key, summarizeValue(value))
	}

	fmt.Println("\n📥 Final Store (after processing):")
	for key, value := range result.FinalStore {
		fmt.Printf("  %s: %s\n", key, summarizeValue(value))
	}
	fmt.Println()

	// Show processing results
	fmt.Println("🔍 === PROCESSING RESULTS ===")

	// Configuration results
	if env, ok := result.FinalStore["current_environment"].(string); ok {
		fmt.Printf("✅ Environment: %s\n", env)
	}

	if features, ok := result.FinalStore["enabled_features"].([]interface{}); ok {
		fmt.Printf("✅ Enabled Features: %v\n", features)
	}

	// Processing statistics
	if stats, ok := result.FinalStore["transformation_stats"].(map[string]interface{}); ok {
		fmt.Printf("✅ Processing Stats:\n")
		for key, value := range stats {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}

	// Final report
	if report, ok := result.FinalStore["final_report"].(map[string]interface{}); ok {
		fmt.Printf("✅ Final Report:\n")

		if businessMetrics, ok := report["business_metrics"].(map[string]interface{}); ok {
			fmt.Printf("  Business Metrics:\n")
			for key, value := range businessMetrics {
				fmt.Printf("    %s: %v\n", key, value)
			}
		}

		if recommendations, ok := report["recommendations"].([]interface{}); ok {
			fmt.Printf("  Recommendations:\n")
			for i, rec := range recommendations {
				fmt.Printf("    %d. %v\n", i+1, rec)
			}
		}
	}

	fmt.Println()
	fmt.Println("🎉 Store Workflow Example completed successfully!")
	fmt.Println("   ✅ Demonstrated passing initial store data to workflow")
	fmt.Println("   ✅ Showed multi-stage data processing with store")
	fmt.Println("   ✅ Retrieved comprehensive final store state")
	fmt.Println("   ✅ Illustrated store-based configuration and results")
}

func summarizeValue(value interface{}) string {
	switch v := value.(type) {
	case map[string]interface{}:
		return fmt.Sprintf("map[%d keys]", len(v))
	case []interface{}:
		return fmt.Sprintf("array[%d items]", len(v))
	case string:
		if len(v) > 30 {
			return fmt.Sprintf("'%s...' (%d chars)", v[:27], len(v))
		}
		return fmt.Sprintf("'%s'", v)
	case int, int64, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%v", v)
	default:
		str := fmt.Sprintf("%v", v)
		if len(str) > 30 {
			return fmt.Sprintf("%s... (%T)", str[:27], v)
		}
		return fmt.Sprintf("%v (%T)", v, v)
	}
}
