# Store Workflow Example

This example demonstrates the new **InitialStore** and **FinalStore** functionality in gostage for normal (non-spawned) workflows.

## What This Example Shows

### 🎯 **Core Features Demonstrated**

1. **Initial Store Setup** - Pass configuration and input data to workflows
2. **Store-based Configuration** - Actions read config from store and set processing parameters  
3. **Multi-stage Data Processing** - Transform data through multiple stages using store
4. **Final Store Retrieval** - Get complete final state after workflow execution
5. **Store Data Flow** - See how data evolves from initial → processed → final

### 📊 **Workflow Structure**

```
Initial Store (3 keys)
    ↓
Stage 1: Configuration Processing
    ↓ (reads app_config, sets processing parameters)
Stage 2: Data Transformation  
    ↓ (processes input_data using config parameters)
Stage 3: Report Generation
    ↓ (analyzes results and generates report)
Final Store (20+ keys)
```

### 🔧 **Actions Overview**

#### **ConfigProcessorAction**
- Reads `app_config` from initial store
- Sets environment-specific parameters (`debug_mode`, `batch_size`, `log_level`)
- Processes database configuration and feature flags
- Stores processed configuration for other actions

#### **DataTransformerAction**  
- Uses configuration set by ConfigProcessorAction
- Processes `input_data` in batches based on `batch_size`
- Transforms records based on type (user, product, order)
- Applies categorization and calculations
- Stores transformation results and statistics

#### **ReportGeneratorAction**
- Analyzes all processed data
- Generates business metrics and data quality scores
- Provides processing recommendations
- Creates comprehensive final report

### 💾 **Store Data Flow**

| **Stage** | **Reads From Store** | **Writes To Store** |
|-----------|---------------------|-------------------|
| **Initial** | - | `app_config`, `input_data`, `metadata` |
| **Config** | `app_config` | `current_environment`, `debug_mode`, `batch_size`, `log_level`, `db_*`, `enabled_features` |
| **Transform** | `batch_size`, `debug_mode`, `input_data` | `transformed_data`, `transformation_stats` |
| **Report** | All previous data | `final_report` |
| **Final** | - | 20+ keys with complete processing state |

## 🚀 **How to Run**

```bash
cd examples/store_workflow
go run main.go
```

## 📋 **Expected Output**

The example will show:

1. **Initial Setup** - 3 store keys with config and input data
2. **Processing Logs** - Configuration processing, data transformation, report generation
3. **Store Transformation** - Before/after view of store contents
4. **Processing Results** - Environment settings, statistics, business metrics
5. **Final Report** - Data quality analysis and recommendations

## 🎯 **Key Concepts Illustrated**

### **Using ExecuteWithOptions with InitialStore**
```go
options := gostage.RunOptions{
    Logger:       gostage.NewDefaultLogger(),
    Context:      context.Background(), 
    IgnoreErrors: false,
    InitialStore: initialStore,  // ← Pass initial data
}

result := gostage.RunWorkflow(workflow, options)
```

### **Accessing Final Store Results**
```go
// Access final store after execution
fmt.Printf("Final store keys: %d\n", len(result.FinalStore))

// Get specific processed data
if report, ok := result.FinalStore["final_report"].(map[string]interface{}); ok {
    // Use the final report data
}
```

### **Store-based Action Communication**
```go
// Action reads config from store
if config, err := store.Get[map[string]interface{}](ctx.Workflow.Store, "app_config"); err == nil {
    // Process config and set parameters
    ctx.Workflow.Store.Put("batch_size", 10)
}

// Later action uses the config
batchSize, _ := store.GetOrDefault[int](ctx.Workflow.Store, "batch_size", 10)
```

## 🆚 **Comparison with Spawn Example**

| **Store Workflow** | **Spawn Process** |
|-------------------|-------------------|
| Single process | Multi-process |
| Direct store access | Store + IPC messaging |
| Synchronous execution | Asynchronous with communication |
| Simple store flow | Store export/import |
| In-memory sharing | Cross-process serialization |

## 🎁 **Benefits of Store-based Workflows**

- ✅ **Configuration Management** - Centralized config accessible to all actions
- ✅ **Data Persistence** - Results preserved throughout workflow execution  
- ✅ **Action Coordination** - Actions can build upon previous results
- ✅ **Debugging** - Complete state visible in final store
- ✅ **Testing** - Easy to inspect intermediate and final states
- ✅ **Flexibility** - Dynamic behavior based on store contents

This example provides a comprehensive demonstration of how to use the new store functionality for powerful, data-driven workflow execution! 