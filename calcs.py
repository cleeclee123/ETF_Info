from scipy.optimize import newton
import math

# Define the function to calculate the present value of the bond
def bond_price(YTM, face_amount, coupon_yield, market_value, T, n=1):
    C = face_amount * coupon_yield / n
    price = C * (1 - (1 + YTM / n) ** (-n * T)) / (YTM / n) + face_amount * (
        1 + YTM / n
    ) ** (-n * T)
    return price - market_value


def calculate_YTM(face_amount, coupon_yield, market_value, T, n=1):
    # Make an initial guess for YTM (we use the coupon yield as an initial guess)
    initial_guess = coupon_yield
    try:
        ytm = newton(
            bond_price,
            initial_guess,
            args=(face_amount, coupon_yield, market_value, T, n),
        )
        return ytm * n
    except Exception as e:
        print(f"Failed to converge. Error: {e}")
        return None


def calculate_macaulay_duration(face_amount, coupon_yield, market_value, T, YTM, n=1):
    C = face_amount * coupon_yield / n
    macaulay_duration = (
        C * (1 - (1 + YTM / n) ** (-n * T)) / (YTM / n)
        + face_amount * (1 + YTM / n) ** (-n * T)
    ) / market_value

    return macaulay_duration

# need to get benchmark data
def get_benchmark_data(ticker: str):
    

def dts_calc(duration: int, spread: int):
    return abs(duration * spread)



if __name__ == "__main__":
    face_amount = 74483351
    coupon_yield = 0.0363
    market_value = 23274498.68
    T = 2035 - 2023
    n = 1

    ytm = calculate_YTM(face_amount, coupon_yield, market_value, T, n)
    if ytm:
        print(f"The Yield to Maturity is {ytm*100:.4f}%")

        macaulay_duration = calculate_macaulay_duration(
            face_amount, coupon_yield, market_value, T, ytm, n
        )
        print(f"The Macaulay Duration is {macaulay_duration:.6f} years")
