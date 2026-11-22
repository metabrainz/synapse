package store

// Namespace keeps environments that share a Redis server apart by prefixing
// every key and pub/sub channel name. The zero value leaves names unchanged.
type Namespace string

// Name returns name inside the namespace.
func (n Namespace) Name(name string) string {
	if n == "" {
		return name
	}
	return string(n) + ":" + name
}
