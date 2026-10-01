namespace TxMonitoring.Shared;

/// <summary>
/// The one versioned transform from integer minor units to the floating-point feature the
/// spike detector consumes. The artifact builder and the runtime both call it. The original
/// long stays the only monetary value.
/// </summary>
public static class AmountFeatures
{
    /// <summary>
    /// log(1 + AmountMinor), which tames skewed amounts. Monotonic and finite over the whole
    /// minor-unit range (log(1 + 1e11) ≈ 25.3). It is not summed as money.
    /// </summary>
    public static float FromAmountMinor(long amountMinor) =>
        (float)Math.Log(1.0 + amountMinor);
}
