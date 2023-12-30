package main

import "sync"

type Future interface {
	Result() (string, error)
}

type InnerFuture struct {
	once sync.Once
	wg   sync.WaitGroup

	res   string
	err   error
	resCh <-chan string
	errCh <-chan error
}

func (f *InnerFuture) Result() (string, error) {
	f.once.Do(
		func() {
			f.wg.Add(1)
			defer f.wg.Done()

			f.res = <-f.resCh
			f.err = <-f.errCh

		})
	f.wg.Wait()

	return f.res, f.err
}

func main() {

}
