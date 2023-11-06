import numpy as np
from scipy.optimize import newton
from datetime import datetime


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
            return "Error"

    # @staticmethod
    # def calc_YTM(par, coupon_yield, market_value, T, n):
    #     C = par * coupon_yield / n
    #     ytm_initial_guess = coupon_yield  # Using the coupon rate as an initial guess for YTM
    #     periods = n * T  # Total number of periods

    #     # Objective function to minimize (difference between calculated and actual bond price)
    #     def objective_function(ytm):
    #         return (
    #             sum(C / ((1 + ytm/n)**(t)) for t in range(1, int(periods) + 1)) +
    #             (par / ((1 + ytm/n)**periods))
    #         ) - market_value

    #     # Using a numerical solver to find the YTM that minimizes the difference between calculated and actual bond price
    #     from scipy.optimize import newton
    #     ytm = newton(objective_function, ytm_initial_guess)
    #     return ytm

    @staticmethod
    def calc_current_yield(par, coupon_yield, market_value):
        annual_coupon_payment = coupon_yield * par
        return annual_coupon_payment / market_value

    @staticmethod
    def calc_macaulay_duration(par, coupon_yield, market_value, T, YTM, n=1):
        try:
            C = par * coupon_yield / n
            # macaulay_duration = (
            #     C * (1 - (1 + YTM / n) ** (-n * T)) / (YTM / n)
            #     + par * (1 + YTM / n) ** (-n * T)
            # ) / market_value

            duration_numerator = 0
            for t in range(1, int(T) + 1):
                duration_numerator += (C / ((1 + YTM) ** t)) * t

            duration_numerator += (par / ((1 + YTM) ** T)) * T
            macaulay_duration = duration_numerator / market_value

            return macaulay_duration
        except Exception as e:
            print(e)
            return "Error"

    @staticmethod
    def simple_macaulay_duration(y, C, F, T, P):
        cash_flows = [(t, C) for t in range(1, int(T) + 1)]
        cash_flows.append((T, F + C))  # Adding the final cash flow
        return sum(t * cf / ((1 + y) ** t) for t, cf in cash_flows) / P

    @staticmethod
    def calc_modifed_duration(par, coupon_yield, market_value, T, n=1):
        try:
            ytm = Bond.calc_YTM(par, coupon_yield, market_value, T, n)
            macaulay_duration = Bond.calc_macaulay_duration(
                par, coupon_yield, market_value, T, ytm, n
            )

            return macaulay_duration / (1 + (ytm / n))
        except Exception as e:
            print(e)
            return "Error"

    @staticmethod
    def simple_modified_duration(macaulay_dur, y, m=1):
        return macaulay_dur / (1 + y / m)

    @staticmethod
    def calc_convexity(par, coupon_yield, market_value, T, n=1):
        # try:
        #     C = par * coupon_yield / n
        #     ytm = Bond.calc_YTM(par, coupon_yield, market_value, T, n) / n
        #     P = market_value
        #     convexity_sum = sum(
        #         (t * (t + 1) * C) / ((1 + ytm / n)**(t))
        #         for t in range(1, int(n * T) + 1)
        #     )
        #     terminal_part = (T * (T + 1) * par) / ((1 + ytm / n)**(2 * n * T))
        #     convexity = (convexity_sum + terminal_part) / (P * (1 + ytm / n)**2)

        #     return convexity
        # except Exception as e:
        #     print(e)
        #     return 'Error'

        try:
            YTM = Bond.calc_YTM(par, coupon_yield, market_value, T, n) / n
            coupon_payment = coupon_yield * par
            convexity = 0
            for t in range(1, int(T) + 1):
                convexity += (coupon_payment * t * (t + 1)) / ((1 + YTM) ** t)

            convexity += (par * T * (T + 1)) / ((1 + YTM) ** T)
            convexity = convexity / ((1 + YTM) ** 2 * par)

            return convexity
        except Exception as e:
            print(e)
            return "Error"

    @staticmethod
    def calc_remaining_time_to_maturity(T, current_period, n=1):
        return T - (current_period / n)

    @staticmethod
    def calc_market_price_per_face_value(market_value, par):
        return market_value / par

    @staticmethod
    def calc_accrued_interest(
        face_amount, coupon_yield, periods_since_last_payment, n=1
    ):
        return (coupon_yield * face_amount / n) * periods_since_last_payment

    @staticmethod
    def calc_clean_and_dirty_price(
        market_value, face_amount, coupon_yield, periods_since_last_payment, n=1
    ):
        accrued_int = Bond.accrued_interest(
            face_amount, coupon_yield, periods_since_last_payment, n
        )
        dirty_price = market_value
        clean_price = market_value - accrued_int
        return clean_price, dirty_price

    @staticmethod
    def calc_capital_gain_loss(market_value, purchase_price):
        return market_value - purchase_price

    @staticmethod
    def simple_bond_price(fv, c, ytm, m, t):
        return ((fv * c / m * (1 - (1 + ytm / m) ** (-m * t))) / (ytm / m)) + fv * (
            1 + (ytm / m)
        ) ** (-m * t)

    @staticmethod
    def calc_time_to_maturity(date_string):
        try:
            maturity_date = datetime.strptime(date_string, "%m/%d/%Y")
            current_date = datetime.now()
            days_to_maturity = (maturity_date - current_date).days
            years_to_maturity = days_to_maturity / 365.0
            return years_to_maturity
        except ValueError as e:
            print(f"Invalid date format: {e}")
            return None
        
    
    @staticmethod
    def calc_time_to_maturity_blk(date_string):
        try:
            maturity_date = datetime.strptime(date_string, "%b %d, %Y")
            current_date = datetime.now()
            days_to_maturity = (maturity_date - current_date).days
            years_to_maturity = days_to_maturity / 365.0
            return years_to_maturity
        except ValueError as e:
            print(f"Invalid date format: {e}")
            return None


class ZeroCouponBond:
    @staticmethod
    def calc_YTM(par, market_value, T):
        YTM = (par / market_value) ** (1 / T) - 1
        return YTM

    @staticmethod
    def calc_macaulay_duration(T):
        return T

    @staticmethod
    def calc_modified_duration(years_to_maturity, yield_to_maturity):
        return years_to_maturity / (1 + yield_to_maturity)

    @staticmethod
    def calc_convexity(ttm, ytm):
        return ((ttm ** 2) +( ttm / 2)) / ((1 + (ytm / 2)) ** (2 + 2 * ttm))

    @staticmethod
    def calc_market_price(par, YTM, T):
        market_price = par / (1 + YTM) ** T
        return market_price

    @staticmethod
    def durations(cfs, rates, price, ytm, no_coupons, payments_per_year=2):
        mac_dur = (
            np.sum(
                [
                    cfs[i]
                    * ((i + 1) / payments_per_year)
                    / np.power(1 + rates[i], i + 1)
                    for i in range(len(cfs))
                ]
            )
            / price
        )
        mod_dur = mac_dur / (1 + ytm / no_coupons)
        return mac_dur, mod_dur


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


=SUM(X2:X82)/SUM(L2:L82)
=SUM(Y2:Y82)/SUM(L2:L82)
=SUM(Z2:Z82)/SUM(L2:L82)
    

"""
