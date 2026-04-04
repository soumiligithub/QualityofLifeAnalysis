# extract/loan_extractor.py
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import json
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoanDataExtractor:
    """
    Extract home loan data for major Indian cities and banks
    Data includes interest rates, EMI, processing fees, and audience types
    """
    
    def __init__(self):
        # Major Indian cities for analysis
        self.cities = [
            'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai',
            'Kolkata', 'Pune', 'Ahmedabad', 'Jaipur', 'Lucknow',
            'Chandigarh', 'Bhopal', 'Indore', 'Nagpur', 'Surat',
            'Visakhapatnam', 'Patna', 'Kochi', 'Coimbatore', 'Vadodara',
            'Thane', 'Agra', 'Ranchi', 'Bhubaneswar'
        ]
        
        # Major banks in India
        self.banks = {
            'Public Sector': ['SBI', 'PNB', 'BOB', 'Canara Bank', 'Union Bank', 
                            'Indian Bank', 'Bank of India', 'Central Bank'],
            'Private Sector': ['HDFC', 'ICICI', 'Axis Bank', 'Kotak Mahindra', 
                             'Yes Bank', 'IDFC First', 'IndusInd Bank'],
            'Housing Finance': ['LIC Housing Finance', 'HDFC Ltd', 'PNB Housing', 
                              'DHFL', 'GIC Housing', 'IndiaBulls Housing']
        }
        
        # Audience categories based on profession/income
        self.audience_types = [
            'Salaried - IT/Corporate',
            'Salaried - Government/PSU',
            'Self-Employed - Professional',
            'Self-Employed - Business',
            'Self-Employed - SME',
            'Self-Employed - Startup',
            'Women Applicants',
            'Senior Citizens',
            'NRIs',
            'First Time Home Buyers'
        ]
        
        self.output_dir = Path('data/raw/')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def get_sbi_loan_rates(self):
        """Get SBI home loan rates (example of scraping)"""
        # Note: In production, you'd scrape actual websites
        # For now, creating realistic data based on SBI's actual rate structure
        
        sbi_rates = {
            'Salaried': {
                'up_to_30_lakhs': 8.65,
                '30_lakhs_to_75_lakhs': 8.75,
                'above_75_lakhs': 8.85
            },
            'Non-Salaried': {
                'up_to_30_lakhs': 8.70,
                '30_lakhs_to_75_lakhs': 8.80,
                'above_75_lakhs': 8.90
            },
            'Women': {
                'up_to_30_lakhs': 8.60,
                '30_lakhs_to_75_lakhs': 8.70,
                'above_75_lakhs': 8.80
            }
        }
        return sbi_rates
    
    def generate_loan_data(self, bank_name, bank_type, city):
        """
        Generate realistic loan data for a specific bank and city
        Based on actual Indian home loan market conditions
        """
        # Base rates vary by bank type and city
        if bank_type == 'Public Sector':
            base_rate = np.random.uniform(8.4, 9.2)
        elif bank_type == 'Private Sector':
            base_rate = np.random.uniform(8.3, 9.0)
        else:  # Housing Finance
            base_rate = np.random.uniform(8.5, 9.4)
        
        # City premium/discount
        city_premium = {
            'Mumbai': 0.15, 'Delhi': 0.10, 'Bangalore': 0.10,
            'Hyderabad': 0.05, 'Chennai': 0.05, 'Pune': 0.05,
            'Kolkata': -0.10, 'Ahmedabad': -0.05, 'Jaipur': -0.05
        }
        premium = city_premium.get(city, 0)
        
        # Audience-specific rates
        audience_data = []
        
        for audience in self.audience_types:
            # Adjust rate based on audience type
            if 'Women' in audience:
                rate_adjustment = -0.05  # 0.05% lower for women
            elif 'Government' in audience:
                rate_adjustment = -0.10  # 0.10% lower for govt employees
            elif 'Senior' in audience:
                rate_adjustment = 0.10  # 0.10% higher for senior citizens
            elif 'NRI' in audience:
                rate_adjustment = 0.20  # 0.20% higher for NRIs
            elif 'Startup' in audience:
                rate_adjustment = 0.15  # 0.15% higher for startups
            elif 'Salaried' in audience and 'IT' in audience:
                rate_adjustment = -0.05  # Slightly lower for IT professionals
            else:
                rate_adjustment = 0
            
            interest_rate = round(base_rate + premium + rate_adjustment, 2)
            
            # Calculate EMI for different loan amounts
            loan_scenarios = []
            for loan_amount in [2500000, 5000000, 7500000, 10000000]:
                for tenure in [15, 20, 25, 30]:
                    monthly_rate = interest_rate / 12 / 100
                    months = tenure * 12
                    
                    # EMI formula: P * r * (1+r)^n / ((1+r)^n - 1)
                    if monthly_rate > 0:
                        emi = loan_amount * monthly_rate * (1 + monthly_rate)**months / ((1 + monthly_rate)**months - 1)
                    else:
                        emi = loan_amount / months
                    
                    loan_scenarios.append({
                        'loan_amount': loan_amount,
                        'tenure_years': tenure,
                        'emi_amount': round(emi, 0),
                        'total_interest': round(emi * months - loan_amount, 0)
                    })
            
            # Calculate affordability based on typical salary in city
            avg_salary = self.get_city_avg_salary(city)
            affordable_loan = self.calculate_affordable_loan(avg_salary, interest_rate)
            
            audience_data.append({
                'bank_name': bank_name,
                'bank_type': bank_type,
                'city': city,
                'audience_type': audience,
                'interest_rate': interest_rate,
                'processing_fee': round(np.random.uniform(0.25, 0.75), 2),  # % of loan amount
                'max_loan_amount': int(np.random.uniform(5000000, 15000000)),
                'min_loan_amount': 300000,
                'min_tenure_years': 5,
                'max_tenure_years': 30,
                'prepayment_charges': np.random.choice([0, 2, 3], p=[0.7, 0.2, 0.1]),
                'foreclosure_charges': np.random.choice([0, 2, 3, 4], p=[0.5, 0.3, 0.15, 0.05]),
                'avg_salary_required': int(avg_salary * 0.4),  # EMI shouldn't exceed 40% of salary
                'affordable_loan_amount': affordable_loan,
                'special_offers': self.get_special_offers(bank_name, audience),
                'scraped_date': datetime.now().strftime('%Y-%m-%d')
            })
        
        return audience_data
    
    def get_city_avg_salary(self, city):
        """Get average monthly salary for city (in INR)"""
        salaries = {
            'Mumbai': 55000, 'Delhi': 52000, 'Bangalore': 58000,
            'Hyderabad': 50000, 'Chennai': 48000, 'Kolkata': 45000,
            'Pune': 51000, 'Ahmedabad': 46000, 'Jaipur': 42000,
            'Lucknow': 40000, 'Chandigarh': 48000, 'Bhopal': 38000,
            'Indore': 39000, 'Nagpur': 38000, 'Surat': 42000,
            'Visakhapatnam': 38000, 'Patna': 35000, 'Kochi': 43000,
            'Coimbatore': 40000, 'Vadodara': 39000, 'Thane': 45000,
            'Agra': 36000, 'Ranchi': 35000, 'Bhubaneswar': 37000
        }
        return salaries.get(city, 40000)
    
    def calculate_affordable_loan(self, monthly_salary, interest_rate):
        """
        Calculate affordable loan amount based on salary
        Assumes EMI shouldn't exceed 40% of monthly salary
        """
        max_emi = monthly_salary * 0.4
        tenure_months = 20 * 12  # 20 years typical
        monthly_rate = interest_rate / 12 / 100
        
        # Reverse EMI formula to calculate loan amount
        # P = EMI * ((1+r)^n - 1) / (r * (1+r)^n)
        if monthly_rate > 0:
            loan_amount = max_emi * ((1 + monthly_rate)**tenure_months - 1) / (monthly_rate * (1 + monthly_rate)**tenure_months)
        else:
            loan_amount = max_emi * tenure_months
        
        return int(loan_amount)
    
    def get_special_offers(self, bank_name, audience):
        """Generate special offers based on bank and audience"""
        offers = []
        
        # Bank-specific offers
        if 'SBI' in bank_name:
            offers.append("No processing fee for women applicants")
            offers.append("0.05% concession for CASA customers")
        elif 'HDFC' in bank_name:
            offers.append("Balance transfer facility at lower rates")
            offers.append("Free credit card with home loan")
        elif 'ICICI' in bank_name:
            offers.append("Quick approval within 3 days")
            offers.append("Flexible EMI options")
        
        # Audience-specific offers
        if 'Women' in audience:
            offers.append("0.05% interest concession")
            offers.append("Reduced processing fee")
        elif 'Government' in audience:
            offers.append("Special rates for govt employees")
            offers.append("Quick disbursal")
        elif 'NRI' in audience:
            offers.append("Special NRI home loan schemes")
            offers.append("Dedicated relationship manager")
        elif 'First Time' in audience:
            offers.append("Subsidy under PMAY scheme")
            offers.append("Free legal consultation")
        
        return ', '.join(offers[:3]) if offers else "Standard terms apply"
    
    def extract_all_loans(self):
        """Main extraction function"""
        logger.info("Starting loan data extraction...")
        
        all_loan_data = []
        
        # Generate data for each bank and city
        total_banks = sum(len(banks) for banks in self.banks.values())
        total_combinations = total_banks * len(self.cities)
        
        logger.info(f"Extracting data for {total_banks} banks across {len(self.cities)} cities")
        logger.info(f"Total combinations: {total_combinations}")
        
        counter = 0
        for bank_type, banks in self.banks.items():
            for bank_name in banks:
                for city in self.cities:
                    counter += 1
                    if counter % 100 == 0:
                        logger.info(f"Processed {counter}/{total_combinations} combinations")
                    
                    loan_data = self.generate_loan_data(bank_name, bank_type, city)
                    all_loan_data.extend(loan_data)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_loan_data)
        
        # Add unique ID
        df['loan_id'] = range(1, len(df) + 1)
        
        # Reorder columns
        column_order = ['loan_id', 'bank_name', 'bank_type', 'city', 'audience_type',
                       'interest_rate', 'processing_fee', 'min_loan_amount', 'max_loan_amount',
                       'min_tenure_years', 'max_tenure_years', 'prepayment_charges',
                       'foreclosure_charges', 'avg_salary_required', 'affordable_loan_amount',
                       'special_offers', 'scraped_date']
        
        df = df[column_order]
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = self.output_dir / f'home_loans_{timestamp}.csv'
        df.to_csv(output_file, index=False)
        
        # Also save a summary
        summary = self.generate_summary(df)
        summary_file = self.output_dir / f'loan_summary_{timestamp}.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"✅ Extracted {len(df)} loan records")
        logger.info(f"✅ Data saved to: {output_file}")
        logger.info(f"✅ Summary saved to: {summary_file}")
        
        return df
    
    def generate_summary(self, df):
        """Generate summary statistics"""
        summary = {
            'extraction_date': datetime.now().isoformat(),
            'total_records': len(df),
            'unique_banks': df['bank_name'].nunique(),
            'unique_cities': df['city'].nunique(),
            'unique_audience_types': df['audience_type'].nunique(),
            'avg_interest_rate': df['interest_rate'].mean(),
            'avg_processing_fee': df['processing_fee'].mean(),
            'interest_rate_by_bank_type': df.groupby('bank_type')['interest_rate'].mean().to_dict(),
            'cheapest_cities': df.groupby('city')['interest_rate'].mean().nsmallest(5).to_dict(),
            'costliest_cities': df.groupby('city')['interest_rate'].mean().nlargest(5).to_dict(),
            'audience_best_rates': df.groupby('audience_type')['interest_rate'].mean().nsmallest(5).to_dict()
        }
        return summary
    
    def create_emi_calculator_dataset(self, df):
        """Create a separate dataset for EMI calculations"""
        logger.info("Creating EMI calculator dataset...")
        
        emi_data = []
        for _, row in df.iterrows():
            for loan_amount in [3000000, 5000000, 7500000]:
                for tenure in [15, 20, 25]:
                    monthly_rate = row['interest_rate'] / 12 / 100
                    months = tenure * 12
                    
                    if monthly_rate > 0:
                        emi = loan_amount * monthly_rate * (1 + monthly_rate)**months / ((1 + monthly_rate)**months - 1)
                    else:
                        emi = loan_amount / months
                    
                    total_payment = emi * months
                    total_interest = total_payment - loan_amount
                    
                    emi_data.append({
                        'bank_name': row['bank_name'],
                        'city': row['city'],
                        'audience_type': row['audience_type'],
                        'interest_rate': row['interest_rate'],
                        'loan_amount': loan_amount,
                        'tenure_years': tenure,
                        'emi_amount': round(emi, 0),
                        'total_interest': round(total_interest, 0),
                        'total_payment': round(total_payment, 0)
                    })
        
        emi_df = pd.DataFrame(emi_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        emi_file = self.output_dir / f'emi_calculations_{timestamp}.csv'
        emi_df.to_csv(emi_file, index=False)
        
        logger.info(f"✅ EMI calculations saved to: {emi_file}")
        return emi_df

# Run the extractor
if __name__ == "__main__":
    extractor = LoanDataExtractor()
    
    # Extract loan data
    loan_df = extractor.extract_all_loans()
    
    # Create EMI calculator dataset
    emi_df = extractor.create_emi_calculator_dataset(loan_df)
    
    # Display sample
    print("\n" + "="*60)
    print("SAMPLE EXTRACTED DATA")
    print("="*60)
    print(loan_df.head(10))
    
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(f"Total Records: {len(loan_df)}")
    print(f"Unique Banks: {loan_df['bank_name'].nunique()}")
    print(f"Unique Cities: {loan_df['city'].nunique()}")
    print(f"Average Interest Rate: {loan_df['interest_rate'].mean():.2f}%")
    print(f"Lowest Interest Rate: {loan_df['interest_rate'].min():.2f}%")
    print(f"Highest Interest Rate: {loan_df['interest_rate'].max():.2f}%")
    
    print("\n" + "="*60)
    print("INTEREST RATES BY AUDIENCE TYPE")
    print("="*60)
    audience_rates = loan_df.groupby('audience_type')['interest_rate'].mean().sort_values()
    for audience, rate in audience_rates.head(5).items():
        print(f"{audience}: {rate:.2f}%")