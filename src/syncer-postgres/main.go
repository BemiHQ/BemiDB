package main

func main() {
	config := LoadConfig()
	syncer := NewSyncer(config)
	syncer.Sync()
}
