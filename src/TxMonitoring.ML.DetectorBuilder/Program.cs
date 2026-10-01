using System.Runtime.CompilerServices;

using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.Transforms.TimeSeries;

using TxMonitoring.Shared;

// The output path is relative to the source tree, not the build output directory.
string GetSourceDir([CallerFilePath] string path = "") => Path.GetDirectoryName(path)!;

var localDir = GetSourceDir();
var modelPath = Path.GetFullPath(
    Path.Combine(localDir, "..", "TxMonitoring.Detection.Worker", "model.zip")
);

Console.WriteLine("Building online IID spike-detector configuration for the amount feature...");

var mlContext = new MLContext();

// DetectIidSpike is an online detector. Fit validates the schema but learns nothing from rows,
// so one representative value is enough to build the transformer. The worker warms each
// account's state from recent Postgres rows through the same AmountFeatures transform.
var schemaData = mlContext.Data.LoadFromEnumerable(
    new[] { new AmountObservation { AmountFeature = AmountFeatures.FromAmountMinor(2_500) } }
);

var pipeline = mlContext.Transforms.DetectIidSpike(
    outputColumnName: nameof(SpikePrediction.Prediction),
    inputColumnName: nameof(AmountObservation.AmountFeature),
    confidence: 90.0,
    pvalueHistoryLength: 20
);

var model = pipeline.Fit(schemaData);

mlContext.Model.Save(model, schemaData.Schema, modelPath);
Console.WriteLine($"Model saved to: {modelPath}");

Console.WriteLine("\n--- Testing Model Locally ---");
var predictionEngine = model.CreateTimeSeriesEngine<AmountObservation, SpikePrediction>(mlContext);

// Warm the engine with 50 steady $25-$30 points. The spread is deterministic so regenerating
// the artifact repeats the same check. A constant history would have zero variance and flag
// any jitter as a spike.
for (int i = 0; i < 50; i++)
{
    predictionEngine.Predict(
        new AmountObservation { AmountFeature = AmountFeatures.FromAmountMinor(2_500 + ((i * 37) % 500)) }
    );
}

// Normal (steady $25-equivalent)
var normalResult = predictionEngine.Predict(
    new AmountObservation { AmountFeature = AmountFeatures.FromAmountMinor(2_600) }
);
Console.WriteLine($"Normal amount (2600) -> Is Spike? {normalResult.Prediction[0] == 1}");

// Test Spike ($4,000-equivalent, far outside the warmed baseline)
var spikeResult = predictionEngine.Predict(
    new AmountObservation { AmountFeature = AmountFeatures.FromAmountMinor(400_000) }
);
Console.WriteLine($"Spike amount (400000) -> Is Spike? {spikeResult.Prediction[0] == 1}");

public class AmountObservation
{
    [LoadColumn(0)]
    public float AmountFeature { get; set; }
}

public class SpikePrediction
{
    [VectorType(3)]
    public double[] Prediction { get; set; } = default!;
}
