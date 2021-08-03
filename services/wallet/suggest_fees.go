package wallet

import (
	"fmt"
	"math"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type FeeHistoryResult struct {
	OldestBlock  uint64           `json:"oldestBlock"`
	Reward       [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee      []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio []float64        `json:"gasUsedRatio"`
}

type FeeSuggestion struct {
	MaxFeePerGas         *big.Float `json:"maxFeePerGas"`
	MaxPriorityFeePerGas *big.Float `json:"maxPriorityFeePerGas"`
}

func (s *Service) SuggestFees() ([]*FeeSuggestion, error) {
	// feeHistory API call without a reward percentile specified is cheap even with a light client backend because it only needs block headers.
	// Therefore we can afford to fetch a hundred blocks of base fee history in order to make meaningful estimates on variable time scales.
	result := &FeeHistoryResult{}
	err := s.rpcClient.Call(&result, "eth_feeHistory", 100, "latest", nil)
	if err != nil {
		return nil, err
	}

	var baseFees []*big.Float
	var order []int
	for i, e := range result.BaseFee {
		baseFees = append(baseFees, big.NewFloat(float64((*big.Int)(e).Uint64())))
		order = append(order, i)
	}

	// If a block is full then the baseFee of the next block is copied. The reason is that in full blocks the minimal tip might not be enough to get included.
	// The last (pending) block is also assumed to end up being full in order to give some upwards bias for urgent suggestions.

	lastElement := baseFees[len(baseFees)-1]
	lastElement.Mul(lastElement, big.NewFloat(9))
	lastElement.Quo(lastElement, big.NewFloat(8))

	for i := len(result.GasUsedRatio) - 1; i >= 0; i-- {
		if result.GasUsedRatio[i] > 0.9 {
			baseFees[i] = big.NewFloat(0).Copy(baseFees[i+1])
		}
	}

	sort.SliceStable(order, func(i, j int) bool {
		return baseFees[order[i]].Cmp(baseFees[order[j]]) < 0
	})

	var sorted []*big.Float
	for _, i := range order {
		sorted = append(sorted, big.NewFloat(0).Copy(baseFees[i]))
	}

	tip, err := s.suggestTip(result.OldestBlock, result.GasUsedRatio)
	if err != nil {
		return nil, err
	}

	maxBaseFee := big.NewFloat(0)

	var maxTimeFactor float64 = 15

	var extraTipRatio = big.NewFloat(0.25)

	response := make([]*FeeSuggestion, int(maxTimeFactor+1))

	for timeFactor := int(maxTimeFactor); timeFactor >= 0; timeFactor-- {
		bf := suggestBaseFee(baseFees, order, float64(timeFactor))
		t := big.NewFloat(float64(tip.Int64()))
		if bf.Cmp(maxBaseFee) == 1 {
			maxBaseFee = big.NewFloat(0).Copy(bf)
		} else {
			// If a narrower time window yields a lower base fee suggestion than a wider window then we are probably in a price dip.
			// In this case getting included with a low tip is not guaranteed; instead we use the higher base fee suggestion
			// and also offer extra tip to increase the chance of getting included in the base fee dip.

			tempMaxBaseFee := big.NewFloat(0)
			tempMaxBaseFee.Sub(maxBaseFee, bf)
			tempMaxBaseFee.Mul(tempMaxBaseFee, extraTipRatio)
			t.Add(t, tempMaxBaseFee)
			bf = big.NewFloat(0).Copy(maxBaseFee)
		}
		response[timeFactor] = &FeeSuggestion{
			MaxFeePerGas:         big.NewFloat(0).Add(bf, t),
			MaxPriorityFeePerGas: t,
		}
	}

	return response, nil
}

func (s *Service) suggestTip(firstBlock uint64, gasUsedRatio []float64) (*big.Int, error) {
	ptr := len(gasUsedRatio) - 1
	needBlocks := 5
	var rewards []*big.Int
	for needBlocks > 0 && ptr >= 0 {
		blockCount := maxBlockCount(gasUsedRatio, ptr, needBlocks)
		if blockCount > 0 {
			feeHistory := &FeeHistoryResult{}
			err := s.rpcClient.Call(&feeHistory, "eth_feeHistory", blockCount, fmt.Sprintf("0x%x", firstBlock+uint64(ptr)), []int{10})
			if err != nil {
				return big.NewInt(0), err
			}
			for i := range feeHistory.Reward {
				rewards = append(rewards, (*big.Int)(feeHistory.Reward[i][0]))
			}

			if len(feeHistory.Reward) < blockCount {
				break
			}
			needBlocks -= blockCount
		}
		ptr -= blockCount + 1
	}

	if len(rewards) == 0 {
		return big.NewInt(5e9), nil
	}

	sort.Slice(rewards, func(i, j int) bool {
		return rewards[i].Cmp(rewards[j]) < 0
	})

	return rewards[int(math.Floor(float64(len(rewards))/2))], nil
}

// maxBlockCount returns the number of consecutive blocks suitable for tip suggestion (gasUsedRatio between 0.1 and 0.9).
func maxBlockCount(gasUsedRatio []float64, ptr int, needBlocks int) int {
	blockCount := 0
	for needBlocks > 0 && ptr >= 0 {
		if gasUsedRatio[ptr] < 0.1 || gasUsedRatio[ptr] > 0.9 {
			break
		}
		ptr--
		needBlocks--
		blockCount++
	}
	return blockCount
}

func suggestBaseFee(baseFee []*big.Float, order []int, timeFactor float64) *big.Float {
	if timeFactor < 1e-6 {
		return baseFee[len(baseFee)-1]
	}
	pendingWeight := (1 - math.Exp(-1/timeFactor)) / (1 - math.Exp(-float64(len(baseFee))/timeFactor))
	var sumWeight float64
	result := big.NewFloat(0)
	var samplingCurveLast float64
	for i := 0; i < len(order); i++ {
		sumWeight += pendingWeight * math.Exp(float64(order[i]-len(baseFee)+1)/timeFactor)
		var samplingCurveValue = samplingCurve(sumWeight)
		multipliedSampling := big.NewFloat(samplingCurveValue - samplingCurveLast)
		multipliedSampling.Mul(multipliedSampling, baseFee[order[i]])
		result.Add(result, multipliedSampling)
		if samplingCurveValue >= 1 {
			return result
		}
		samplingCurveLast = samplingCurveValue
	}
	return result
}

// samplingCurve is a helper function for the base fee percentile range calculation.
func samplingCurve(sumWeight float64) float64 {

	sampleMin := 0.1
	sampleMax := 0.3
	if sumWeight <= sampleMin {
		return 0
	}
	if sumWeight >= sampleMax {
		return 1
	}
	return (1 - math.Cos((sumWeight-sampleMin)*2*math.Pi/(sampleMax-sampleMin))) / 2
}
