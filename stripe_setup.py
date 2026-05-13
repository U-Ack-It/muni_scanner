"""
Stripe setup — creates the $299/month MuniScanner subscription product
and prints a shareable payment link.

Usage:
    python3 stripe_setup.py          # create product + price + payment link
    python3 stripe_setup.py --link   # print existing payment link only

Add to .env:
    STRIPE_SECRET_KEY=sk_live_...   (or sk_test_... for testing)
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

import stripe

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

if not stripe.api_key:
    print("ERROR: STRIPE_SECRET_KEY not set in .env")
    print("  Get it from: https://dashboard.stripe.com/apikeys")
    sys.exit(1)

PRODUCT_NAME  = "BondAnomaly — Weekly Municipal Bond Anomaly Alerts"
PRICE_USD     = 29900  # cents = $299.00
CURRENCY      = "usd"
INTERVAL      = "month"


def get_or_create_product() -> str:
    products = stripe.Product.list(active=True, limit=20)
    for p in products.data:
        if p.name == PRODUCT_NAME:
            print(f"  Product exists: {p.id}")
            return p.id
    product = stripe.Product.create(
        name=PRODUCT_NAME,
        description=(
            "Weekly curated municipal bond anomaly alerts. Each alert includes "
            "yield spread vs rated peers, call risk analysis, EMMA link, and "
            "AI-generated credit research. Cancel anytime."
        ),
    )
    print(f"  Created product: {product.id}")
    return product.id


def get_or_create_price(product_id: str) -> str:
    prices = stripe.Price.list(product=product_id, active=True, limit=10)
    for p in prices.data:
        if p.unit_amount == PRICE_USD and p.recurring and p.recurring.interval == INTERVAL:
            print(f"  Price exists: {p.id} (${PRICE_USD/100:.2f}/{INTERVAL})")
            return p.id
    price = stripe.Price.create(
        product=product_id,
        unit_amount=PRICE_USD,
        currency=CURRENCY,
        recurring={"interval": INTERVAL},
    )
    print(f"  Created price: {price.id} (${PRICE_USD/100:.2f}/{INTERVAL})")
    return price.id


def create_payment_link(price_id: str) -> str:
    link = stripe.PaymentLink.create(
        line_items=[{"price": price_id, "quantity": 1}],
        after_completion={
            "type": "redirect",
            "redirect": {"url": "https://olivierboukli.com/thank-you"},
        },
        billing_address_collection="auto",
        phone_number_collection={"enabled": False},
    )
    return link.url


def main():
    print("Setting up MuniScanner subscription on Stripe...\n")
    product_id  = get_or_create_product()
    price_id    = get_or_create_price(product_id)
    link        = create_payment_link(price_id)

    print(f"\n{'='*55}")
    print(f"  PAYMENT LINK (share this):")
    print(f"  {link}")
    print(f"{'='*55}")
    print(f"\nPaste this link in your reply email when a firm says yes.")
    print(f"Stripe dashboard: https://dashboard.stripe.com/subscriptions")

    # Save link to file for use in outreach emails
    link_file = Path(__file__).parent / "stripe_payment_link.txt"
    link_file.write_text(link)
    print(f"\nLink saved to: {link_file}")


if __name__ == "__main__":
    main()
