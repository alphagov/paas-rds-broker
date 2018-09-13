package helpers

import "sync"

type WaitFunc func()
type CleanFunc func()
type ProvisionFunc func() (WaitFunc, CleanFunc)

// ProvisionManager performs the provisioning.
// It gets  a Provisioner function that should do the creationg and
// returns:
//  - a function that would synchronously wait for the thing to finish
//  - a function that would clean up whatever is created
//
// It allows to optionally wait for the creation to finish or cleanup only once.
type ProvisionManager struct {
	Provisioner   ProvisionFunc
	cleaner       CleanFunc
	wg            sync.WaitGroup
	provisionOnce sync.Once
	cleanupOnce   sync.Once
}

func (p *ProvisionManager) Provision() {
	p.provisionOnce.Do(func() {
		waitFunc, cleaner := p.Provisioner()
		p.cleaner = cleaner
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			waitFunc()
		}()
	})
}

func (p *ProvisionManager) Wait() {
	p.wg.Wait()
}

func (p *ProvisionManager) CleanUp() {
	p.Wait()
	p.cleanupOnce.Do(func() {
		p.cleaner()
	})
}
