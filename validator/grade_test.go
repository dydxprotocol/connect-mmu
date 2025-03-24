package validator

import (
	"testing"

	mmtypes "github.com/skip-mev/connect/v2/x/marketmap/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/validator/types"
)

func ptrFloat64(f float64) *float64 {
	return &f
}

func TestCheckZScore(t *testing.T) {
	const ticker = "TEST/USD"
	check := CheckZScore(1.5)

	tests := []struct {
		zScore float64
		want   bool
	}{
		{0.0, false},
		{1.0, false},
		{1.5, false},
		{1.6, true},
		{-1.5, false},
		{-1.6, true},
	}

	for _, tt := range tests {
		report := types.ProviderReport{ZScore: tt.zScore}
		got := check(ticker, report)
		require.Equal(t, tt.want, got, "CheckZScore(%v)", tt.zScore)
	}
}

func TestCheckSuccessThreshold(t *testing.T) {
	ticker := "TEST/USD"
	check := CheckSuccessThreshold(0.8) // bound at 0.8

	tests := []struct {
		successRate float64
		want        bool
	}{
		{1.0, false},
		{0.9, false},
		{0.8, false},
		{0.79, true},
		{0.0, true},
	}

	for _, tt := range tests {
		report := types.ProviderReport{SuccessRate: tt.successRate}
		got := check(ticker, report)
		require.Equal(t, tt.want, got, "CheckSuccessThreshold(%v)", tt.successRate)
	}
}

func TestCheckReferencePrice(t *testing.T) {
	v := &Validator{}
	ticker := "TEST/USD"
	check := v.CheckReferencePrice(0.05) // bound at 0.05

	value1 := 0.04
	value2 := 0.05
	value3 := 0.06

	tests := []struct {
		referencePriceDiff *float64
		want               bool
	}{
		{nil, false},
		{&value1, false},
		{&value2, false},
		{&value3, true},
	}

	for _, tt := range tests {
		report := types.ProviderReport{ReferencePriceDiff: tt.referencePriceDiff}
		got := check(ticker, report)
		require.Equal(t, tt.want, got, "CheckReferencePrice(%v)", tt.referencePriceDiff)
	}
}

func TestGradeReports(t *testing.T) {
	v := &Validator{}

	// Create provider reports
	pr1 := types.ProviderReport{
		Name:               "Provider1",
		ZScore:             1.0,
		SuccessRate:        0.9,
		AveragePrice:       100.0,
		ReferencePriceDiff: ptrFloat64(0.04),
	}
	pr2 := types.ProviderReport{
		Name:               "Provider2",
		ZScore:             2.0,
		SuccessRate:        0.7,
		AveragePrice:       101.0,
		ReferencePriceDiff: ptrFloat64(0.06),
	}
	pr3 := types.ProviderReport{
		Name:               "Provider3",
		ZScore:             -2.0,
		SuccessRate:        0.95,
		AveragePrice:       99.0,
		ReferencePriceDiff: nil,
	}

	report := types.Report{
		Ticker:          "TEST/USD",
		ProviderReports: []types.ProviderReport{pr1, pr2, pr3},
	}

	reports := []types.Report{report}

	zScoreCheck := CheckZScore(1.5)
	successRateCheck := CheckSuccessThreshold(0.8)
	referencePriceCheck := v.CheckReferencePrice(0.05)

	summary := v.GradeReports(reports, zScoreCheck, successRateCheck, referencePriceCheck)

	expectedGrades := []string{
		types.GradePassed, // pr1
		types.GradeFailed, // pr2 (fails on ZScore and SuccessRate)
		types.GradeFailed, // pr3 (fails on ZScore)
	}

	for i, providerReport := range summary.Reports[0].ProviderReports {
		require.Equal(t, expectedGrades[i], providerReport.Grade, "Provider %s grade", providerReport.Name)
	}

	expectedPassingRatio := "1/3"
	require.Equal(t, expectedPassingRatio, summary.Reports[0].PassingRatio, "PassingRatio")
}

func TestGradeReportsWithFlexibleRefPrice(t *testing.T) {
	mm := mmtypes.MarketMap{Markets: make(map[string]mmtypes.Market)}
	v := New(mm, zap.NewNop(), WithFlexibleRefPriceMarkets([]string{"VOLATILE_TEST/USD"}))
	refPriceBound := 0.05

	normalPr1 := types.ProviderReport{
		Name:               "Provider1",
		ZScore:             0.5,
		SuccessRate:        0.9,
		AveragePrice:       100.0,
		ReferencePriceDiff: ptrFloat64(0.06),
	}
	normalPr2 := types.ProviderReport{
		Name:               "Provider2",
		ZScore:             0.2,
		SuccessRate:        0.95,
		AveragePrice:       101.0,
		ReferencePriceDiff: ptrFloat64(0.04),
	}

	normalReport := types.Report{
		Ticker:          "TEST/USD",
		ProviderReports: []types.ProviderReport{normalPr1, normalPr2},
	}

	// Create provider reports for volatile market
	volatilePr1 := types.ProviderReport{
		Name:               "Provider1",
		ZScore:             0.5,
		SuccessRate:        0.9,
		AveragePrice:       100.0,
		ReferencePriceDiff: ptrFloat64(0.06), // below doubled configured bound
	}
	volatilePr2 := types.ProviderReport{
		Name:               "Provider2",
		ZScore:             0.2,
		SuccessRate:        0.95,
		AveragePrice:       101.0,
		ReferencePriceDiff: ptrFloat64(0.11), // above doubled configured bound
	}

	volatileReport := types.Report{
		Ticker:          "VOLATILE_TEST/USD",
		ProviderReports: []types.ProviderReport{volatilePr1, volatilePr2},
	}

	reports := []types.Report{normalReport, volatileReport}

	zScoreCheck := CheckZScore(1.5)
	successRateCheck := CheckSuccessThreshold(0.8)
	referencePriceCheck := v.CheckReferencePrice(refPriceBound)

	summary := v.GradeReports(reports, zScoreCheck, successRateCheck, referencePriceCheck)

	// Check normal market grades
	expectedNormalGrades := []string{
		types.GradeFailed, // should fail on ref price
		types.GradePassed, // should pass all checks
	}

	for i, providerReport := range summary.Reports[0].ProviderReports {
		require.Equal(t, expectedNormalGrades[i], providerReport.Grade, "Normal market provider %s grade", providerReport.Name)
	}

	// Check volatile market grades
	expectedVolatileGrades := []string{
		types.GradePassed, // should pass with doubled threshold
		types.GradeFailed, // should fail even with doubled threshold
	}

	for i, providerReport := range summary.Reports[1].ProviderReports {
		require.Equal(t, expectedVolatileGrades[i], providerReport.Grade, "Volatile market provider %s grade", providerReport.Name)
	}

	require.Equal(t, "1/2", summary.Reports[0].PassingRatio, "Normal market passing ratio")
	require.Equal(t, "1/2", summary.Reports[1].PassingRatio, "Volatile market passing ratio")
}
