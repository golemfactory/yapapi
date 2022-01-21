from yapapi.strategy import MarketStrategy

DEFAULT_OFFER_SCORE = 77.51937984496124  # this matches the OfferProposalFactory default


class Always6(MarketStrategy):
    async def score_offer(self, offer):
        return 6
