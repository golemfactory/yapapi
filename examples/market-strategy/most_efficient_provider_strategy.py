from collections import defaultdict

from yapapi.strategy import MarketStrategy, SCORE_TRUSTED


class MostEfficientProviderStrategy(MarketStrategy):
    def __init__(self):
        self.history = defaultdict(list)

    async def score_offer(self, offer, _agreements_pool):
        provider_id = offer.issuer
        previous_runs = self.history[provider_id]

        if not previous_runs:
            #   We always want to try a new provider
            score = SCORE_TRUSTED
            print(f"Found new provider: {provider_id}, default score {SCORE_TRUSTED}")
        else:
            #   Faster provider is a better provider
            avg_time = sum(previous_runs)/len(previous_runs)
            score = SCORE_TRUSTED - avg_time
            print(f"Scored known provider: {provider_id}: {score} ({len(previous_runs)} runs, avg time {avg_time})")

        return score

    def save_execution_time(self, provider_id: str, time: float):
        self.history[provider_id].append(time)
