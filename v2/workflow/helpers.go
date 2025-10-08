package workflow

func contains(values []string, target string) bool {
    for _, v := range values {
        if v == target {
            return true
        }
    }
    return false
}

func hasAny(values []string, targets []string) bool {
    for _, v := range values {
        for _, t := range targets {
            if v == t {
                return true
            }
        }
    }
    return false
}
