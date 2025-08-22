package common

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	set := make(Set[T])
	return set
}

func (set Set[T]) Add(item T) Set[T] {
	set[item] = struct{}{}
	return set
}

func (set Set[T]) AddAll(items []T) Set[T] {
	for _, item := range items {
		set.Add(item)
	}
	return set
}

func (set Set[T]) Remove(item T) Set[T] {
	delete(set, item)
	return set
}

func (set Set[T]) Contains(item T) bool {
	_, ok := set[item]
	return ok
}

func (set Set[T]) Values() []T {
	values := make([]T, 0, len(set))
	for val := range set {
		values = append(values, val)
	}

	return values
}

func (set Set[T]) Reset() {
	for key := range set {
		delete(set, key)
	}
}

func (set Set[T]) IsEmpty() bool {
	return len(set) == 0
}
