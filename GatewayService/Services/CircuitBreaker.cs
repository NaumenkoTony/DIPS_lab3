namespace GatewayService.Services;

public class CircuitBreaker(int failureThreshold, TimeSpan timeout)
{
    private enum CircuitBreakerState
    {
        Closed,
        Open,
        HalfOpen
    }

    private int failureCount = 0;
    private readonly int failureThreshold = failureThreshold;
    private readonly TimeSpan timeout = timeout;
    private DateTime lastAttemptTime = DateTime.UtcNow;
    private CircuitBreakerState state = CircuitBreakerState.Closed;

    public bool IsOpen => state == CircuitBreakerState.Open;
    public bool IsHalfOpen => state == CircuitBreakerState.HalfOpen;

    public void RecordSuccess()
    {
        failureCount = 0;
        state = CircuitBreakerState.Closed;
    }

    public void RecordFailure()
    {
        failureCount++;
        if (failureCount >= failureThreshold)
        {
            state = CircuitBreakerState.Open;
            lastAttemptTime = DateTime.UtcNow;
        }
    }

    public bool AllowRequest()
    {
        if (state == CircuitBreakerState.Open)
        {
            if (DateTime.UtcNow - lastAttemptTime >= timeout)
            {
                state = CircuitBreakerState.HalfOpen;
                return true;
            }
            return false;
        }
        return true;
    }

    public void Reset()
    {
        failureCount = 0;
        state = CircuitBreakerState.Closed;
    }
}
