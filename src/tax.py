# Databricks notebook source
"""
Tax & Realization Calculation Helper for DHT Cement
"""
# 18% VAT + 2.5% SSCL
TAX_MULTIPLIER = 1.2095

def calculate_net_revenue(gross_revenue: float, tax_multiplier: float = TAX_MULTIPLIER) -> float:
    """Gross revenue divided by VAT & SSCL"""
    return gross_revenue / tax_multiplier if gross_revenue else 0.0

def calculate_bag_net_realization(gross_price: float, cash_rebate: float, bag_transport: float, tax_multiplier: float = TAX_MULTIPLIER) -> float:
    """
    Net Realization per bag = (Gross - Rebate) / 1.2095 - Transport
    """
    return ((gross_price - cash_rebate) / tax_multiplier) - bag_transport

def calculate_bulk_net_realization(gross_price: float, bulk_rebate: float, commission: float, bulk_transport: float, tax_multiplier: float = TAX_MULTIPLIER) -> float:
    """
    Bulk NR per MT = (Gross - Rebate - Commission) / 1.2095 - Transport
    """
    return ((gross_price - bulk_rebate - commission) / tax_multiplier) - bulk_transport
