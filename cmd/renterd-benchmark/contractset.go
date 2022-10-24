package main

// TODO:
// - use Sia Central SDK instead of scraping siastats.info

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/types"

	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

type (
	hostInfo struct {
		Id                 uint64
		Online             bool
		Pubkey             string
		CurrentIp          string
		CountryCode        interface{}
		TotalStorage       float64
		AcceptingContracts bool
		Version            string
		UsedStorage        float64
		Collateral         uint64
		ContractPrice      uint64
		StoragePrice       uint64
		UploadPrice        uint64
		DownloadPrice      uint64
		Rank               uint64
		Cheating           interface{}
		FinalScore         uint64
		ErrorType          interface{}
	}
)

func ensureContracts(c *api.Client, renterKey api.PrivateKey, required int) ([]api.Contract, error) {
	log.Println("- ensuring contracts...")

	// assert we have enough contracts
	contracts, err := fetchContracts(c, renterKey)
	if err != nil {
		log.Fatal("failed fetching contacts, err:", err)
	}

	// nothing to do
	if len(contracts) >= required {
		return contracts, nil
	}

	// create a map of host keys for existing contracts
	contractMap := map[string]struct{}{}
	for _, contract := range contracts {
		contractMap[contract.HostKey.String()] = struct{}{}
	}

	// create the required set of contracts
	hosts, err := fetchHostInfo()
	if err != nil {
		return nil, fmt.Errorf("failed fetching host info, err: %v", err)
	}

	missing := required - len(contracts)
	log.Printf("- %v contracts missing\n", missing)

	// warn user if outputs are too few to form missing contracts
	outputs, err := c.WalletOutputs()
	if err != nil {
		log.Fatal("failed fetching outputs, err:", err)
	}
	if missing > len(outputs) {
		log.Println("- not enough outputs to form missing contracts")
	}

	var h int
	for missing > 0 && h < len(hosts) {
		hostPubkey := hosts[h].Pubkey
		hostIP := hosts[h].CurrentIp
		h++

		_, exists := contractMap[hostPubkey]
		if exists {
			continue
		}

		var hostKey api.PublicKey
		err := hostKey.UnmarshalText([]byte(hostPubkey))
		if err != nil {
			log.Printf("failed to unmarshal host pubkey %v, err %v\n", hostPubkey, err)
			continue
		}

		// fetch the tip
		tip, err := c.ConsensusTip()
		if err != nil {
			return nil, fmt.Errorf("failed contacting renterd API, err: %v", err)
		}

		log.Printf("- forming contract with %v %v\n", hostKey.String(), hostIP)

		contract, err := createContract(c, tip.Height, renterKey, hostKey, hostIP)
		if err != nil && strings.Contains(err.Error(), "insufficient balance") {
			// before waiting for pending transactions, always check whether we
			// miraculously have enough contracts all of a sudden due to pending
			// transactions that came through
			curr, _ := fetchContracts(c, renterKey)
			if len(curr) >= required {
				return curr, nil
			}

			log.Println("- waiting for pending transactions...")
			err = waitForPendingTransactions(c)
			if err != nil {
				return nil, err
			}

			// h-- // retry current host
			continue
		} else if err != nil {
			log.Printf("failed to create contract host pubkey %v, err %v\n", hostPubkey, err)
			break
		}

		missing--

		err = c.AddContract(contract)
		if err != nil {
			log.Printf("failed to add contract, err %v", err)
			continue
		}

		contracts = append(contracts, api.Contract{
			ID:        contract.ID(),
			HostKey:   hostKey,
			HostIP:    hostIP,
			RenterKey: renterKey,
		})
	}

	return contracts, nil
}

func fetchContracts(c *api.Client, renterKey api.PrivateKey) ([]api.Contract, error) {
	// fetch the tip
	tip, err := c.ConsensusTip()
	if err != nil {
		return nil, fmt.Errorf("failed contacting renterd API, err: %v", err)
	}

	// fetch all hosts
	hosts, err := fetchHostInfo()
	if err != nil {
		return nil, fmt.Errorf("failed fetching host info, err: %v", err)
	}

	// build host IP map
	hostIPs := make(map[string]string)
	for _, host := range hosts {
		hostIPs[host.Pubkey] = host.CurrentIp
	}

	// fetch the contracts
	allContracts, err := c.Contracts()
	if err != nil {
		return nil, fmt.Errorf("failed fetching contacts, err: %v", err)
	}

	renterPK := renterKey.PublicKey()

	// filter out unsable contracts
	var contracts []api.Contract
	for _, contract := range allContracts {

		// extract renter pk
		var contractPK api.PublicKey
		copy(contractPK[:], contract.Revision.UnlockConditions.PublicKeys[0].Key)

		// a contract is unusable if it was not created by this renter, it is
		// too close to the proof window start or if no renter funds remain
		notFromRenter := !bytes.Equal(renterPK[:], contractPK[:])
		tooCloseToProofWindow := tip.Height >= uint64(contract.Revision.NewWindowStart)-144
		noFundsRemaining := contract.Revision.NewValidProofOutputs[0].Value.IsZero()
		if notFromRenter || tooCloseToProofWindow || noFundsRemaining {
			continue
		}

		hostIP, found := hostIPs[contract.HostKey().String()]
		if !found {
			// log.Println("could not find IP for host", contract.HostKey().String())
			continue
		}

		contracts = append(contracts, api.Contract{
			ID:        contract.ID(),
			HostKey:   contract.HostKey(),
			HostIP:    hostIP,
			RenterKey: renterKey,
		})
	}
	return contracts, nil
}

func fetchHostInfo() ([]hostInfo, error) {
	params := url.Values{}
	params.Set("network", "sia")
	params.Set("list", "active")

	resp, err := http.PostForm("https://siastats.info/hosts-api/allhosts", params)
	if err != nil {
		return nil, errors.AddContext(err, "failed fetching hosts from siastats")
	}

	var hosts []hostInfo
	err = json.NewDecoder(resp.Body).Decode(&hosts)
	if err != nil {
		return nil, errors.AddContext(err, "failed decoding response from siastats")
	}

	// sort the hosts by rank
	sort.Slice(hosts, func(i, j int) bool {
		return hosts[i].Rank < hosts[j].Rank
	})

	return hosts, nil
}

func waitForPendingTransactions(c *api.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	tip, err := c.ConsensusTip()
	if err != nil {
		return err
	}

	utxos, err := c.WalletOutputs()
	if err != nil {
		return err
	}
	if len(utxos) == 0 {
		return errors.New("no unspent outputs")
	}

	interval := 10 * time.Second
	timer := time.NewTimer(interval)
	for {
		txns, err := c.WalletPending()
		if err != nil {
			return err
		}

		updated, err := c.ConsensusTip()
		if err != nil {
			return err
		}

		if updated.Height > tip.Height && len(txns) > 0 {
			return fmt.Errorf("new block at %v, %v transactions still pending", updated.Height, len(txns))
		}

		select {
		case <-timer.C:
			timer.Reset(interval)
		case <-ctx.Done():
			return nil
		}
	}
}

func createContract(c *api.Client, currentHeight uint64, renterKey api.PrivateKey, hostKey api.PublicKey, hostIP string) (rhpv2.Contract, error) {
	settings, err := c.RHPScan(hostKey, hostIP)
	if err != nil {
		return rhpv2.Contract{}, fmt.Errorf("failed to get host settings for host %v %v, err %v", hostKey, hostIP, err)
	}

	const storageBytes = 25 * rhpv2.SectorSize // 1GiB
	const durationBlocks = 30 * 144            // 1 month
	var endHeight = currentHeight + durationBlocks

	var maxPrice = types.SiacoinPrecision.Mul64(100e3) // 100KS
	renterFunds := (settings.StoragePrice.Add(settings.UploadBandwidthPrice)).Mul64(storageBytes).Mul64(durationBlocks)
	if renterFunds.Cmp(maxPrice) > 0 {
		renterFunds = maxPrice
	}

	hostCollateral := settings.Collateral.Mul64(storageBytes).Mul64(durationBlocks)
	if hostCollateral.Cmp(settings.MaxCollateral) > 0 {
		hostCollateral = settings.MaxCollateral
	}

	addr, _ := c.WalletAddress()
	fc, cost, err := c.RHPPrepareForm(renterKey, hostKey, renterFunds, addr, hostCollateral, endHeight, settings)
	if err != nil {
		return rhpv2.Contract{}, fmt.Errorf("failed to prepare form for host %v %v, err %v", hostKey, hostIP, err)
	}
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}

	toSign, parents, err := c.WalletFund(&txn, cost)
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(
			c.WalletDiscard(txn),
			fmt.Errorf("failed to fund the transaction for host %v %v, err %v", hostKey, hostIP, err),
		)
	}

	err = c.WalletSign(&txn, toSign, wallet.ExplicitCoveredFields(txn))
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(
			c.WalletDiscard(txn),
			fmt.Errorf("failed to sign the transaction for host %v %v, err %v", hostKey, hostIP, err),
		)
	}
	contract, _, err := c.RHPForm(renterKey, hostKey, hostIP, append(parents, txn))
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(
			c.WalletDiscard(txn),
			fmt.Errorf("failed to form the contract for host %v %v, err %v", hostKey, hostIP, err),
		)
	}
	return contract, nil
}
