from scipy.optimize import newton


class Bond:
    @staticmethod
    def calc_YTM(par, coupon_yield, market_value, T, n=1):
        def bond_price(YTM, par, coupon_yield, market_value, T, n=1):
            C = par * coupon_yield / n
            price = C * (1 - (1 + YTM / n) ** (-n * T)) / (YTM / n) + par * (
                1 + YTM / n
            ) ** (-n * T)
            return price - market_value

        initial_guess = coupon_yield
        try:
            ytm = newton(
                bond_price,
                initial_guess,
                args=(par, coupon_yield, market_value, T, n),
            )
            return ytm * n
        except Exception as e:
            print(f"Failed to converge. Error: {e}")
            return None

    @staticmethod
    def calc_current_yield(par, coupon_yield, market_value):
        annual_coupon_payment = coupon_yield * par
        return annual_coupon_payment / market_value

    @staticmethod
    def calc_macaulay_duration(
        par, coupon_yield, market_value, T, YTM, n=1
    ):
        C = par * coupon_yield / n
        macaulay_duration = (
            C * (1 - (1 + YTM / n) ** (-n * T)) / (YTM / n)
            + par * (1 + YTM / n) ** (-n * T)
        ) / market_value

        return macaulay_duration
    
    @staticmethod
    def calc_modifed_duration(par, coupon_yield, market_value, T, n=1):
        ytm = Bond.calc_YTM(par, coupon_yield, market_value, T, n)
        macaulay_duration = Bond.calc_macaulay_duration(
            par, coupon_yield, market_value, T, ytm, n
        )
        
        return macaulay_duration / (1 + (ytm / n))
    
    @staticmethod
    def calc_convexity(par, coupon_yield, market_value, T, n=1):
        C = par * coupon_yield / n
        ytm = Bond.calc_YTM(par, coupon_yield, market_value, T, n) / n
        P = market_value
        convexity_sum = sum(
            (t * (t + 1) * C) / ((1 + ytm / n)**(t))
            for t in range(1, int(n * T) + 1)
        )
        terminal_part = (T * (T + 1) * par) / ((1 + ytm / n)**(2 * n * T))
        convexity = (convexity_sum + terminal_part) / (P * (1 + ytm / n)**2)
        
        return convexity
    
    @staticmethod
    def calc_remaining_time_to_maturity(T, current_period, n=1):
        return T - (current_period / n)
    
    @staticmethod
    def calc_market_price_per_face_value(market_value, par):
        return market_value / par
    
    @staticmethod
    def calc_accrued_interest(face_amount, coupon_yield, periods_since_last_payment, n=1):
        return (coupon_yield * face_amount / n) * periods_since_last_payment
        
    @staticmethod
    def calc_clean_and_dirty_price(market_value, face_amount, coupon_yield, periods_since_last_payment, n=1):
        accrued_int = Bond.accrued_interest(face_amount, coupon_yield, periods_since_last_payment, n)
        dirty_price = market_value
        clean_price = market_value - accrued_int
        return clean_price, dirty_price

    @staticmethod
    def calc_capital_gain_loss(market_value, purchase_price):
        return market_value - purchase_price


"""
Munis: 
Tax-Equivalent Yield - Adjusts the yield of a tax-free municipal bond to a taxable equivalent, considering the investor's tax bracket.
State and Local Tax Implications
Credit Quality - Often assessed using credit rating agencies but specifically for municipalities or the backing entity.

Credit:
Credit Spread - The difference between the yield of the corporate bond and a government bond of similar maturity.
Option Adjusted Spread (OAS) - For bonds with embedded options.
Z-spread
Effective Duration - For bonds with embedded options.
Credit Rating - Assessment of the issuer's creditworthiness from agencies like Moody's, S&P, and Fitch.
Default Probability

MBS:
Prepayment Risk - The risk that homeowners might prepay their mortgages.
Option Adjusted Spread (OAS)
Z-spread
Effective Duration

Treasuries:
Real Yield - Particularly relevant for inflation-linked treasuries (like TIPS in the US).
Interest Rate Risk


// more custom
Interest Rate Risk
Issuer Analysis
"""
