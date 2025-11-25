#!/usr/bin/env python3
"""
Red Flag Analysis Script for New York State Regulations

This script processes individual regulation JSON files and uses GPT-5 mini with structured output
to identify regulatory "red flags" based on the patterns identified by Jennifer Pahlka.
"""

import json
import os
import pandas as pd
from pathlib import Path
from typing import List, Optional
from enum import Enum
from pydantic import BaseModel
import openai
from openai import AsyncOpenAI
from dotenv import load_dotenv
import asyncio
from datetime import datetime

# Load environment variables
load_dotenv()

class RedFlagType(str, Enum):
    """Enum of regulatory red flag patterns identified by Jennifer Pahlka"""
    
    # Complexity & Policy Debt
    CROSS_REFERENCE_SPAGHETTI = "cross_reference_spaghetti"
    ACCRETED_CONDITIONS = "accreted_conditions"
    
    # Options turning into de facto requirements
    SECURITY_CONTROLS_MENU = "security_controls_menu"
    FACTORS_LIST_MANDATORY = "factors_list_mandatory"
    
    # Separation of policy from implementation
    POLICY_TECH_SEPARATION = "policy_tech_separation"
    DESIGN_OPERATIONS_SEPARATION = "design_operations_separation"
    
    # Overwrought legalese / requirement-driven text
    LEGALESE_ELIGIBILITY = "legalese_eligibility"
    REQUIREMENT_DRIVEN_USER_RESEARCH = "requirement_driven_user_research"
    
    # Cascade of rigidity
    TOO_MANY_ABSOLUTE_GOALS = "too_many_absolute_goals"
    COMPLY_WITH_ALL_APPLICABLE = "comply_with_all_applicable"
    AUTOMATIC_PROCESS_RATCHET = "automatic_process_ratchet"
    
    # Mandated steps vs outcomes
    PROCEDURE_HEAVY_DIGITAL = "procedure_heavy_digital"
    STEP_CHECKLIST_NO_METRICS = "step_checklist_no_metrics"
    
    # Administrative burdens on the public
    IN_PERSON_ONLY_RECURRING = "in_person_only_recurring"
    REDUNDANT_HARD_DOCUMENTATION = "redundant_hard_documentation"
    UNREALISTIC_DEADLINES = "unrealistic_deadlines"
    
    # Feedback loops & learning issues
    ONE_SHOT_NO_LEARNING = "one_shot_no_learning"
    
    # Oversight that incentivizes process worship
    IG_RULE_FOLLOWING_ONLY = "ig_rule_following_only"
    
    # Better safe than sorry maximalism
    ZERO_RISK_LANGUAGE = "zero_risk_language"
    SECURITY_ABSOLUTES = "security_absolutes"
    
    # Technology or time-bound requirements
    FROZEN_ARCHITECTURE = "frozen_architecture"

class RegulationAnalysis(BaseModel):
    """Pydantic model for structured regulation analysis output"""
    
    red_flags: List[RedFlagType] = []
    specific_text_examples: List[str] = []
    severity_score: int = 0  # 1-10 scale, we only want 9-10s

class RegulationData(BaseModel):
    """Model for the input regulation data"""
    url: str
    title: str
    content: str
    url_type: str
    source_index: int

def create_analysis_prompt() -> str:
    """Create the prompt for analyzing regulations for red flags"""
    
    return """You are analyzing New York State regulations for problematic patterns that make government less effective for citizens. Your analysis should focus on identifying the REALLY BAD STUFF - the patterns that create unnecessary barriers, complexity, and poor outcomes for people trying to access government services.

Based on Jennifer Pahlka's framework, look for these red flag patterns in the regulatory text:

**COMPLEXITY & POLICY DEBT:**
- Cross-reference spaghetti: Excessive references to other sections, creating complexity
- Accreted conditions: Layer upon layer of added requirements over time

**OPTIONS BECOMING REQUIREMENTS:**
- Security controls "menus" that become checklists in practice
- "Factors to consider" lists that get treated as mandatory requirements

**SEPARATION OF POLICY FROM IMPLEMENTATION:**
- Policy makers completely separated from people who actually deliver services
- Design and operations artificially separated

**OVERWROUGHT LEGALESE:**
- Needlessly complex eligibility language that could be plain English
- Requirements that make user research and feedback practically impossible

**CASCADE OF RIGIDITY:**
- Too many absolute, un-prioritized goals that create impossible trade-offs
- "Comply with all applicable everything" language that creates defensive behavior
- Automatic addition of new procedures after any incident

**MANDATED STEPS VS OUTCOMES:**
- Heavy focus on specific procedures rather than results
- Step-by-step checklists without success metrics

**ADMINISTRATIVE BURDENS:**
- In-person only requirements for things that could be done remotely
- Redundant, hard-to-get documentation requirements
- Unrealistic deadlines that lead to denials by default

**NO LEARNING/FEEDBACK:**
- One-shot programs with no mechanism for improvement
- Oversight focused only on rule-following, not outcomes

**MAXIMALIST SAFETY:**
- Zero-risk language that prevents reasonable trade-offs
- Security requirements with no proportionality

**TECHNOLOGY CONSTRAINTS:**
- Frozen architecture requirements that prevent adaptation

Focus on regulations that would make Jennifer Pahlka say "This is exactly the kind of thing that makes government not work for people."

**CRITICAL: Always provide a severity score 1-10:**
- 1-3: Minor issues, mostly fine
- 4-6: Moderate problems, some barriers to effectiveness  
- 7-8: Significant red flags, likely creates real problems for citizens
- 9-10: Poster child for everything wrong with how government writes rules

**Severity 9-10 indicators (the worst of the worst):**
- Creates impossible catch-22s for citizens
- Requirements that guarantee failure by design
- Cross-reference chains impossible to follow
- Deadlines/processes mathematically impossible to meet
- Systems designed to exhaust people into giving up

Be especially alert to regulations that:
- Create unnecessary complexity for end users
- Prioritize process compliance over helping people
- Make it hard for government workers to use common sense
- Set up systems designed to deny rather than approve
- Prevent learning and iteration

**Always return a severity score even if no red flags are found (score would be 0-1).**"""

async def analyze_regulation(regulation_data: RegulationData, client: AsyncOpenAI) -> RegulationAnalysis:
    """Analyze a single regulation for red flags using GPT-5 nano with structured output"""

    try:
        response = await client.beta.chat.completions.parse(
            model="gpt-5-nano-2025-08-07",
            messages=[
                {"role": "system", "content": create_analysis_prompt()},
                {"role": "user", "content": f"Analyze this regulation content for red flag patterns:\n\n{regulation_data.content}"}
            ],
            response_format=RegulationAnalysis
        )

        return response.choices[0].message.parsed

    except Exception as e:
        print(f"Error analyzing regulation {regulation_data.source_index}: {e}")
        # Return a default analysis in case of error
        return RegulationAnalysis(
            red_flags=[],
            specific_text_examples=[]
        )

def load_regulation_files(directory: str, limit: int = 100) -> List[RegulationData]:
    """Load regulation JSON files from directory"""
    
    regulation_files = list(Path(directory).glob("*.json"))[:limit]
    regulations = []
    
    for file_path in regulation_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                regulations.append(RegulationData(**data))
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue
    
    return regulations

async def analyze_regulations_batch(regulations: List[RegulationData], client: AsyncOpenAI, output_file: str, batch_size: int = 10) -> pd.DataFrame:
    """Analyze a batch of regulations in parallel, saving every 5 completed calls"""

    results = []
    completed_count = 0

    # Process in batches for controlled parallelism
    for batch_start in range(0, len(regulations), batch_size):
        batch = regulations[batch_start:batch_start + batch_size]
        print(f"Processing batch {batch_start // batch_size + 1} ({len(batch)} regulations)...")

        # Create tasks for parallel execution
        tasks = [analyze_regulation(reg, client) for reg in batch]
        analyses = await asyncio.gather(*tasks)

        # Process results from this batch
        for regulation, analysis in zip(batch, analyses):
            row = {
                'source_index': regulation.source_index,
                'title': regulation.title,
                'url': regulation.url,
                'content': regulation.content,
                'url_type': regulation.url_type,
                'red_flags': [flag.value for flag in analysis.red_flags],
                'specific_text_examples': analysis.specific_text_examples,
                'severity_score': analysis.severity_score
            }
            results.append(row)
            completed_count += 1

            print(f"  {regulation.title[:50]}... 🚩 {len(analysis.red_flags)} flags (severity: {analysis.severity_score})")

        # Save to CSV every 5 completed results
        if completed_count % 5 == 0 or batch_start + batch_size >= len(regulations):
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False)
            print(f"  💾 Saved progress ({completed_count} regulations) to {output_file}")

    return pd.DataFrame(results)

async def main():
    """Main execution function"""

    # Initialize async OpenAI client
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Load regulations
    print("Loading regulation files...")
    regulations = load_regulation_files("individual_regulations", limit=100)
    print(f"Loaded {len(regulations)} regulation files")

    if not regulations:
        print("No regulation files found!")
        return

    # Set output file name upfront for incremental saves
    output_file = f"red_flag_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Analyze regulations (batch_size controls parallelism - adjust based on rate limits)
    print("Starting red flag analysis...")
    results_df = await analyze_regulations_batch(regulations, client, output_file, batch_size=10)

    # Final save
    results_df.to_csv(output_file, index=False)
    print(f"Analysis complete! Results saved to {output_file}")
    
    # Print summary statistics
    print(f"\nTotal regulations analyzed: {len(results_df)}")
    print(f"Regulations with red flags: {len(results_df[results_df['red_flags'].str.len() > 0])}")
    print(f"High severity (9-10): {len(results_df[results_df['severity_score'] >= 9])}")
    print(f"Medium severity (7-8): {len(results_df[results_df['severity_score'].between(7, 8)])}")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    asyncio.run(main())