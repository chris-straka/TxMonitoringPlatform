namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Serializes the two transitions that move a MessageId between durable gateway states.
/// </summary>
/// <remarks>
/// SQLite serializes writers, but the idempotency check spans the live queue and the settled
/// markers. Holding this gate across check and commit keeps a late retry from slipping between
/// the uploader's delete and its marker insert.
/// </remarks>
public sealed class BufferMutationGate : IDisposable
{
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public Task EnterAsync(CancellationToken cancellationToken = default) =>
        _semaphore.WaitAsync(cancellationToken);

    public void Exit() => _semaphore.Release();

    public void Dispose() => _semaphore.Dispose();
}
