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
from typing import List, Optional, Dict
from enum import Enum
from pydantic import BaseModel
import openai
from openai import AsyncOpenAI
from dotenv import load_dotenv
import asyncio
from datetime import datetime
import psycopg

# Load environment variables
load_dotenv()

class ComplexityLevel(str, Enum):
    """Complexity levels for implementation issues"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class RedFlagCategory(str, Enum):
    """12 implementation complexity categories based on Jennifer Pahlka's framework"""

    COMPLEXITY_POLICY_DEBT = "complexity_policy_debt"
    OPTIONS_BECOME_REQUIREMENTS = "options_become_requirements"
    POLICY_IMPLEMENTATION_SEPARATION = "policy_implementation_separation"
    OVERWROUGHT_LEGALESE = "overwrought_legalese"
    CASCADE_OF_RIGIDITY = "cascade_of_rigidity"
    MANDATED_STEPS_NOT_OUTCOMES = "mandated_steps_not_outcomes"
    ADMINISTRATIVE_BURDENS = "administrative_burdens"
    NO_FEEDBACK_LOOPS = "no_feedback_loops"
    PROCESS_WORSHIP_OVERSIGHT = "process_worship_oversight"
    ZERO_RISK_LANGUAGE = "zero_risk_language"
    FROZEN_TECHNOLOGY = "frozen_technology"
    IMPLEMENTATION_OPPORTUNITY = "implementation_opportunity"

class RedFlagDetail(BaseModel):
    """Details for a specific red flag category"""
    category: str  # Category name
    explanation: str
    severity: int  # 1-10 scale for this specific category
    complexity: str  # HIGH, MEDIUM, or LOW
    matched_phrases: List[str] = []  # Exact quotes from the regulation
    implementation_approach: str
    effort_estimate: Optional[str] = None  # Rough estimate of effort/timeline
    text_examples: List[str] = []  # Supporting context


class RegulationAnalysis(BaseModel):
    """Pydantic model for structured regulation analysis output"""

    has_implementation_issues: bool  # True if any categories 1-11 are found
    red_flags: List[RedFlagDetail] = []  # List of red flag details
    overall_complexity: Optional[str] = None  # HIGH, MEDIUM, LOW, or None
    summary: Optional[str] = None  # 2-3 sentence summary of main concerns
    requires_technical_review: bool = False  # True if needs architect/engineer review
    has_reporting_requirement: bool = False  # True if regulation creates reporting requirements

class RegulationData(BaseModel):
    """Model for the input regulation data"""
    url: str
    title: str
    content: str
    url_type: str
    source_index: int

def create_analysis_prompt(section_text: str = "", citation: str = "", heading: str = "") -> str:
    """
    Build comprehensive Pahlka implementation analysis prompt with pattern library examples.
    """
    prompt = f"""Analyze this regulation section for implementation complexity using Jennifer Pahlka's framework from "Recoding America".

SECTION INFO:
Citation: {citation}
Heading: {heading}

SECTION TEXT:
{section_text}"""

    prompt += """

---

ANALYSIS FRAMEWORK - 12 CATEGORIES:

1. COMPLEXITY_POLICY_DEBT - Cross-reference spaghetti, accreted conditions
   RED FLAG EXAMPLE: "meets criteria in section 202(b)(3)(A)(ii); satisfies 26 U.S.C. 3304(a)(1)(B)"
   BETTER: "household income is below 150% of federal poverty level"
   DETECT: 3+ cross-references to other sections/USC in a single requirement, or subsection chains 3+ levels deep (e.g., §1-2(a)(3)(B)(ii)), or compound conditions requiring navigation across multiple external sources

2. OPTIONS_BECOME_REQUIREMENTS - Suggestion lists become checklists
   RED FLAG EXAMPLE: "may include...all controls identified in NIST Special Publication 800-53"
   BETTER: "shall adopt controls proportionate to data sensitivity...not required to adopt every listed control"
   DETECT: "may include" followed by exhaustive lists (5+ items) without prioritization, or "may consider" followed by "all of the following", or suggestion language combined with comprehensive external standards

3. POLICY_IMPLEMENTATION_SEPARATION - Waterfall, vendor lock-in
   RED FLAG EXAMPLE: "shall establish through contracts with private entities...shall not directly develop or operate"
   BETTER: "shall designate accountable official responsible for how service works...may use contractors but retain ability to change"
   DETECT: Explicit prohibition on government operation/development, mandatory use of specific vendor types, or separation of policy design from implementation responsibility

4. OVERWROUGHT_LEGALESE - Dense definitions that make forms impossible
   RED FLAG EXAMPLE: "gross unearned income as defined in subsection (k)(2)(B)...per Internal Revenue Code §152"
   BETTER: "ask in plain language about money from work and other sources like Social Security, pensions"
   DETECT: Tax code references, nested definitions, legalese that can't be translated to plain forms

5. CASCADE_OF_RIGIDITY - Conflicting absolute goals
   RED FLAG EXAMPLE: "shall maximize access AND minimize improper payments AND ensure uniform administration AND promote personal responsibility AND avoid any potential misuse"
   BETTER: "main goals: (1) people who qualify can get benefits, (2) limit serious abuse. Balance: don't reduce access unless preventing larger harm"
   DETECT: Multiple unprioritized absolute goals, "comply with all applicable", "ensure", "maximize", "minimize" without trade-offs

6. MANDATED_STEPS_NOT_OUTCOMES - Procedure-heavy vs outcome-focused
   RED FLAG EXAMPLE: "require Form X-100 in PDF format...upload all documents in PDF...issue decision within 90 days of receipt of completed Form X-100"
   BETTER: "decide 90% of complete applications within 10 days...may change forms and formats to improve use"
   DETECT: Hard-coded forms/formats, specific tech requirements (PDF, portals, CMS), step checklists instead of metrics

7. ADMINISTRATIVE_BURDENS - Friction: notaries, wet signatures, in-person
   RED FLAG EXAMPLE: "certified birth certificate...notarized affidavit...appear in person every 3 months...original documents...satisfied within 7 days failing which application shall be denied"
   BETTER: "accept any reasonable proof...use existing government records...recertify by mail/online/phone...30 days to respond...multiple contact attempts before denial"
   DETECT: "notarized", "certified", "original", "in person", "wet signature", "appear", "sworn", recurring appearances, unrealistic deadlines, denial by default

8. NO_FEEDBACK_LOOPS - One-shot design, no iteration
   RED FLAG EXAMPLE: "program authorized for 10 years...Comptroller General shall submit report after 9 years"
   BETTER: "Secretary may run pilots to simplify forms...track completion rates and time to decision...if trial helps people get benefits without major errors, extend nationwide...regularly ask frontline staff which rules cause problems"
   DETECT: No pilot authority, no iteration language, one final report, no user testing, no field feedback mechanisms

9. PROCESS_WORSHIP_OVERSIGHT - Compliance-only audits
   RED FLAG EXAMPLE: "Inspector General shall audit compliance with each requirement...report any deviations however minor...any employee authorizing deviation may be subject to disciplinary action"
   BETTER: "IG shall examine whether program reaches qualifying people AND whether rules are reasonable...when agency departs from procedures, consider whether change helped people get benefits or solved documented problem"
   DETECT: Audit "all requirements", punish "any deviation", no outcome focus, blame-oriented language

10. ZERO_RISK_LANGUAGE - Impossible absolutes
    RED FLAG EXAMPLE: "shall take all necessary measures to ensure that no improper payments occur...ensure that no unauthorized access ever occurs"
    BETTER: "use reasonable steps to reduce errors and abuse, recognizing eliminating every small error would keep qualifying people from help...manage security so serious breaches are unlikely and systems remain available"
    DETECT: "ensure no X occurs", "never", "all necessary measures", "zero", absolutes that ignore trade-offs

11. FROZEN_TECHNOLOGY - Hard-coded architectures, formats, platforms
    RED FLAG EXAMPLE: "create website at www.benefits2025.gov using commercial CMS and enterprise service bus architecture...provide downloadable PDF forms...optimized for desktop web browsers"
    BETTER: "provide online service on .gov domain that allows people to see eligibility, apply, check status...usable on mobile devices...Secretary may change underlying technology"
    DETECT: Named systems/architectures (ESB, specific CMS), hard-coded formats (PDF only), specific URLs, browser requirements, vendor names

12. IMPLEMENTATION_OPPORTUNITY - POSITIVE patterns
    GOOD EXAMPLES:
    - "decide 90% of applications within 10 days" (concrete delivery metric)
    - "Secretary shall design forms so most people can complete on mobile phone" (burden reduction)
    - "Secretary may test simpler ways and keep versions that help qualifying people" (iteration authority)
    - "every 5 years review rules and recommend which can be removed because they add steps without improving results" (cleanup)
    - "identify groups where qualifying people aren't getting benefits and propose changes to close gaps" (outcome focus)
    DETECT: Plain language mandates, concrete metrics, burden reduction, pilot authority, outcome focus, regular cleanup

---

REPORTING REQUIREMENTS DETECTION:

Check if the regulation creates any reporting requirements (binary flag):
- "shall report...", "must submit reports...", "shall file...", "shall provide reports..."
- "shall submit annual/quarterly/monthly reports to..."
- "shall notify...", "shall inform...", "shall document and report..."
- "shall issue reports...", "shall maintain records and report..."

EXAMPLES THAT TRIGGER REPORTING FLAG:
- "shall submit quarterly reports to the Commissioner detailing..."
- "must file annual compliance reports with the Department"
- "shall report all incidents within 24 hours"
- "shall notify stakeholders of policy changes"
- "shall maintain documentation and provide annual reports"

EXAMPLES THAT DON'T:
- "shall follow procedures" (procedural requirement, not reporting)
- "shall maintain records" (recordkeeping only, not reporting)
- "shall consider factors" (analytical requirement, not reporting)

has_reporting_requirement: true if section creates any requirement to submit, file, report, or notify; false otherwise.

---

IMPORTANT: Only flag issues that are CLEAR and SUBSTANTIVE matches to the patterns described above.
- Do not flag minor or tangential instances
- Require strong evidence in the matched_phrases
- Be conservative and precise in classifications
- Avoid over-interpreting routine legal language as problematic
- Focus on patterns that would genuinely create implementation barriers or burdens

For each issue found, provide:
- explanation: [why this creates implementation burden or opportunity, 50-200 chars]
- severity: [1-10 scale where 1-3=LOW, 4-6=MEDIUM, 7-10=HIGH]
- complexity: [HIGH | MEDIUM | LOW based on implementation difficulty]
- matched_phrases: [1-20 exact quotes from section text that demonstrate the issue]
- implementation_approach: [how to implement this better, or what approach is needed]
- effort_estimate: [rough estimate of implementation difficulty/timeline, optional]

Overall assessment:
- has_implementation_issues: true if section has ANY category 1-11 issues (false if only category 12 or no issues)
- overall_complexity: [HIGH | MEDIUM | LOW based on highest individual complexity, null if no issues]
- summary: [2-3 sentences about main implementation concerns or opportunities]
- requires_technical_review: true if section needs architects/engineers to design implementation approach
- has_reporting_requirement: true if section creates reporting/filing/notification requirements, false otherwise

Return VALID JSON with red_flags as an array of objects (each with a category field), plus the overall assessment fields.

Example output:
{
  "has_implementation_issues": true,
  "red_flags": [
    {
      "category": "complexity_policy_debt",
      "explanation": "Contains 47 cross-references making it impossible to follow",
      "severity": 8,
      "complexity": "HIGH",
      "matched_phrases": ["See section 4.1.2(a)(3)"],
      "implementation_approach": "Consolidate into clear statements",
      "text_examples": ["meets criteria in section 202(b)(3)"]
    }
  ],
  "overall_complexity": "HIGH",
  "summary": "Has cross-reference and legalese issues.",
  "requires_technical_review": true,
  "has_reporting_requirement": true
}"""

    return prompt

async def analyze_regulation(regulation_data: RegulationData, client: AsyncOpenAI) -> RegulationAnalysis:
    """Analyze a single regulation for red flags using GPT-5 nano with structured output"""

    try:
        prompt = create_analysis_prompt(
            section_text=regulation_data.content,
            citation=regulation_data.url,
            heading=regulation_data.title
        )

        response = await client.beta.chat.completions.parse(
            model="gpt-5-nano-2025-08-07",
            messages=[
                {"role": "system", "content": prompt}
            ],
            response_format=RegulationAnalysis
        )

        return response.choices[0].message.parsed

    except Exception as e:
        print(f"Error analyzing regulation {regulation_data.source_index}: {e}")
        # Return a default analysis in case of error
        return RegulationAnalysis(
            has_implementation_issues=False,
            red_flags=[],
            overall_complexity=None,
            summary=None,
            requires_technical_review=False,
            has_reporting_requirement=False
        )

def load_regulation_files(directory: str, limit: int = 100, filter_keyword: str = None) -> List[RegulationData]:
    """Load regulation JSON files from directory

    Args:
        directory: Path to directory containing regulation JSON files
        limit: Maximum number of files to load
        filter_keyword: Optional keyword to filter regulations (e.g., 'housing', 'tenant')
    """

    regulation_files = list(Path(directory).glob("*.json"))
    regulations = []

    for file_path in regulation_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

                # Apply keyword filter if specified
                if filter_keyword:
                    title = data.get('title', '').lower()
                    content = data.get('content', '').lower()
                    if filter_keyword.lower() not in title and filter_keyword.lower() not in content:
                        continue

                regulations.append(RegulationData(**data))

                if limit and len(regulations) >= limit:
                    break

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
            # Convert red_flags list to JSON string for CSV storage
            red_flags_json = json.dumps([{
                'category': flag.category,
                'explanation': flag.explanation,
                'severity': flag.severity,
                'complexity': flag.complexity,
                'matched_phrases': flag.matched_phrases,
                'implementation_approach': flag.implementation_approach,
                'effort_estimate': flag.effort_estimate,
                'text_examples': flag.text_examples
            } for flag in analysis.red_flags]) if analysis.red_flags else '[]'

            row = {
                'source_index': regulation.source_index,
                'title': regulation.title,
                'url': regulation.url,
                'content': regulation.content,
                'url_type': regulation.url_type,
                'red_flags': red_flags_json,
                'has_implementation_issues': analysis.has_implementation_issues,
                'overall_complexity': analysis.overall_complexity,
                'summary': analysis.summary,
                'requires_technical_review': analysis.requires_technical_review,
                'has_reporting_requirement': analysis.has_reporting_requirement,
                'max_severity': max([flag.severity for flag in analysis.red_flags], default=0),
                'num_flags': len(analysis.red_flags)
            }
            results.append(row)
            completed_count += 1

            # Format severity summary for console output
            if analysis.red_flags:
                flag_summary = ', '.join([f"{flag.category}:{flag.severity}" for flag in analysis.red_flags])
                print(f"  {regulation.title[:50]}... 🚩 {len(analysis.red_flags)} flags ({flag_summary})")
            else:
                print(f"  {regulation.title[:50]}... No flags")

        # Save to CSV every 5 completed results
        if completed_count % 5 == 0 or batch_start + batch_size >= len(regulations):
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False)
            print(f"  💾 Saved progress ({completed_count} regulations) to {output_file}")

    return pd.DataFrame(results)

async def main(filter_keyword: str = None):
    """Main execution function

    Args:
        filter_keyword: Optional keyword to filter regulations (e.g., 'housing')
    """

    # Initialize async OpenAI client
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Load regulations
    if filter_keyword:
        print(f"Loading regulation files (filtering for '{filter_keyword}')...")
    else:
        print("Loading regulation files...")

    regulations = load_regulation_files("individual_regulations", limit=100, filter_keyword=filter_keyword)
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
    print(f"Regulations with red flags: {len(results_df[results_df['num_flags'] > 0])}")
    print(f"Regulations with high severity (9-10): {len(results_df[results_df['max_severity'] >= 9])}")
    print(f"Regulations with medium severity (7-8): {len(results_df[results_df['max_severity'].between(7, 8)])}")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    import sys

    filter_keyword = None
    if len(sys.argv) > 1:
        filter_keyword = sys.argv[1]

    asyncio.run(main(filter_keyword=filter_keyword))