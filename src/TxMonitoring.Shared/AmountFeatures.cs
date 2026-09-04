namespace TxMonitoring.Shared;

/// <summary>
/// The single versioned transformation from ledger-exact minor units to the bounded
/// floating feature the online spike detector consumes. The artifact builder and the
/// runtime both call this; the original long remains the only monetary value.
/// </summary>
public static class AmountFeatures
{
    /// <summary>
    /// log(1 + AmountMinor): a v1 feature for skewed amounts. Monotonic, finite over the
    /// whole minor-unit range (log(1 + 1e11) ≈ 25.3), and never totalled as money.
    /// </summary>
    public static float FromAmountMinor(long amountMinor) =>
        (float)Math.Log(1.0 + amountMinor);
}
