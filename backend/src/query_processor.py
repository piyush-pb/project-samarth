import logging
import math
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from .data_api_client import DataGovAPIClient
from .gemini_client import GeminiClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


RAIN_DATASET_NAME = "Area-weighted Rainfall Data"
CROP_DATASET_NAME = "District-wise Crop Production"


class QueryProcessor:
    """Core orchestrator for natural language analytics on Indian agri-climate data.

    Combines query parsing (Gemini), data retrieval (data.gov.in), data processing,
    and response generation (Gemini) with rich citations and metadata.
    """

    def __init__(self, data_client: DataGovAPIClient, gemini_client: GeminiClient) -> None:
        self.data_client = data_client
        self.gemini_client = gemini_client

    # -------------------- Public API --------------------
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a user query end-to-end.

        Steps:
        1) Parse query via Gemini
        2) Route to appropriate intent handler
        3) Fetch/process data
        4) Generate grounded response with citations
        5) Return structured payload with answer, sources, data, metadata
        """
        start_time = time.time()
        logger.info("Processing query: %s", user_query)

        parsed = self.gemini_client.parse_query(user_query)
        logger.info("Parsed query result: %s", parsed)

        intent = (parsed.get("intent") or "general").strip()
        logger.info(f"Routing to intent handler: {intent}")

        if intent == "compare_rainfall":
            result = self._compare_rainfall(parsed)
        elif intent == "compare_crops":
            logger.info("Routing to _compare_crops handler")
            result = self._compare_crops(parsed)
        elif intent == "identify_district":
            result = self._identify_district(parsed)
        elif intent == "analyze_trend":
            result = self._analyze_trend(parsed)
        elif intent == "correlation":
            result = self._correlate_climate_crop(parsed)
        elif intent == "policy_analysis":
            result = self._policy_analysis(parsed)
        else:
            # Default: attempt a general analysis with whatever fields we have
            logger.info("Intent 'general' – attempting broad comparison (rainfall/crops) if possible")
            # Try rainfall compare if states present
            states = parsed.get("states") or []
            if states:
                result = self._compare_rainfall(parsed)
            else:
                # Fallback to crops if crops given
                result = self._compare_crops(parsed)

        data_context_obj = self._prepare_data_context(result)
        
        # Convert data_context to string format
        data_context_str = ""
        if isinstance(data_context_obj, dict):
            sources = data_context_obj.get("sources", [])
            notes = data_context_obj.get("notes", "")
            if sources:
                data_context_str = "Sources:\n" + "\n".join(f"  - {s}" for s in sources)
            if notes:
                data_context_str += f"\n\n{notes}"
        
        # Use the full data dict as query_results
        query_results = result.get("data") or {}
        
        answer = self.gemini_client.generate_response(
            user_query=user_query,
            data_context=data_context_str,
            query_results=query_results if isinstance(query_results, dict) else {},
        )

        processing_time = round(time.time() - start_time, 3)
        result.setdefault("metadata", {})
        result["metadata"].update(
            {
                "query_time": datetime.utcnow().isoformat(),
                "processing_time_seconds": processing_time,
            }
        )
        result["answer"] = answer
        logger.info("Response generated in %ss", processing_time)
        return result

    # -------------------- Intent Handlers --------------------
    def _compare_rainfall(self, parsed: Mapping[str, Any]) -> Dict[str, Any]:
        """Compare rainfall statistics between states over a given year range.

        For each state, finds matching meteorological subdivisions, fetches annual rainfall,
        computes averages, and prepares year-by-year breakdowns.
        """
        states: List[str] = [str(s) for s in (parsed.get("states") or [])]
        years = parsed.get("years") or []
        
        # Adjust years to available rainfall data
        adjusted_years = self._get_available_year_range(years, 'rainfall')
        year_start: int = adjusted_years[0]
        year_end: int = adjusted_years[1]
        top_n = int(parsed.get("top_n") or 5)

        print(f"Comparing rainfall for {states} from {year_start} to {year_end}")
        logger.info("Fetching rainfall data for states=%s adjusted_years=%s", states, adjusted_years)

        state_stats: Dict[str, Dict[str, Any]] = {}
        comparisons: List[Dict[str, Any]] = []
        sources: List[Dict[str, Any]] = []
        total_records = 0

        for state in states:
            subdivisions = self._find_matching_subdivisions(state)
            if not subdivisions:
                logger.warning("No subdivisions found for state=%s", state)
                print(f"No meteorological subdivision found for {state}")
                continue
            
            print(f"Found subdivisions for {state}: {subdivisions}")

            yearly_accumulator: Dict[int, List[float]] = defaultdict(list)
            per_subdivision_records = 0

            for subdiv in subdivisions:
                records = self.data_client.fetch_rainfall_data(
                    subdivision=subdiv,
                    year_start=year_start,
                    year_end=year_end,
                    limit=2000,
                )
                per_subdivision_records += len(records)
                for rec in records:
                    try:
                        yr = int(rec.get("year") or rec.get("YEAR"))
                    except Exception:  # noqa: BLE001
                        continue
                    val = rec.get("annual") or rec.get("ANNUAL")
                    if val is not None and _is_number(val):
                        yearly_accumulator[yr].append(float(val))

                # Source entry per subdivision
                sources.append(
                    {
                        "dataset": RAIN_DATASET_NAME,
                        "url": f"{self.data_client.base_url}{self.data_client.RAINFALL_RESOURCE_ID}",
                        "resource_id": self.data_client.RAINFALL_RESOURCE_ID,
                        "filters_applied": {
                            "Subdivision": subdiv,
                            "Years": f"{year_start}-{year_end}" if year_start and year_end else "all",
                        },
                        "records_retrieved": len(records),
                    }
                )

            total_records += per_subdivision_records

            # Average across subdivisions for each year
            per_year: Dict[int, float] = {}
            for y, vals in yearly_accumulator.items():
                if vals:
                    per_year[y] = sum(vals) / len(vals)

            if not per_year:
                logger.warning("No rainfall data aggregated for state=%s", state)
                continue

            years_sorted = sorted(per_year.keys())
            avg_annual = sum(per_year[y] for y in years_sorted) / len(years_sorted)
            stats = self._calculate_rainfall_stats(
                [{"year": y, "annual": per_year[y]} for y in years_sorted]
            )

            state_stats[state] = {
                "average_annual": avg_annual,
                "yearly": {str(y): per_year[y] for y in years_sorted},
                "statistics": stats,
            }
            comparisons.append({"state": state, "average_annual": avg_annual})
            print(f"{state}: Avg rainfall = {avg_annual:.2f}mm ({len(years_sorted)} data points)")

        # Sort for comparison
        comparisons.sort(key=lambda x: x["average_annual"], reverse=True)
        if top_n and top_n > 0:
            comparisons = comparisons[:top_n]

        result = {
            "answer": "",
            "sources": sources,
            "data": {
                "states": state_stats,
                "comparisons": comparisons,
                "statistics": comparisons,  # for summary view in LLM input
            },
            "metadata": {
                "data_sources_queried": len(sources),
                "total_records_processed": total_records,
            },
        }

        return result

    def _compare_crops(self, parsed: Mapping[str, Any]) -> Dict[str, Any]:
        """Compare crop production - groups by crop name and returns top N crops by production.
        
        Fetches all crop records for state/year, groups by crop name, sums production,
        and returns top N crops sorted by production descending.
        """
        print(f"\n\n{'*'*50}")
        print(f"_COMPARE_CROPS CALLED!")
        print(f"Parsed query: {parsed}")
        print(f"{'*'*50}\n\n")
        
        logger.info(f"Starting _compare_crops with parsed_query: {parsed}")

        states: List[str] = [str(s) for s in (parsed.get("states") or [])]
        crops: List[str] = [str(c) for c in (parsed.get("crops") or [])]
        years = parsed.get("years") or []
        top_n = int(parsed.get("top_n") or 5)

        # Adjust years to available crop data
        adjusted_years = self._get_available_year_range(years, 'crop')
        year_start = adjusted_years[0]
        year_end = adjusted_years[1]
        # Use end year for single-year queries
        year = year_end if year_start == year_end else None
        
        print(f"Comparing crops for {states} from {year_start} to {year_end}")

        results: Dict[str, Any] = {}
        sources: List[Dict[str, Any]] = []
        total_records = 0

        for state in states:
            # Fetch data for all years in range
            crop_data: List[Dict[str, Any]] = []
            if year_start == year_end:
                print(f"Fetching data for state: {state}, year: {year_end}")
                crop_data = self.data_client.fetch_crop_production(
                    state=state,
                    year=year_end,
                    crop=None,  # Get all crops
                    limit=10000
                )
            else:
                print(f"Fetching data for state: {state}, years: {year_start} to {year_end}")
                # Fetch for each year in range
                for y in range(year_start, year_end + 1):
                    year_data = self.data_client.fetch_crop_production(
                        state=state,
                        year=y,
                        crop=None,
                        limit=10000
                    )
                    crop_data.extend(year_data)

            print(f"Received {len(crop_data)} records for {state}")
            total_records += len(crop_data)

            if not crop_data:
                year_range_str = str(year_end) if year_start == year_end else f"{year_start}-{year_end}"
                results[state] = {
                    'top_crops': [],
                    'message': f'No data found for {state} in year(s) {year_range_str}'
                }
                continue

            # Group by crop and sum production
            crop_totals: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'production': 0, 'area': 0, 'districts': []})

            for record in crop_data:
                crop_name = record.get('crop') or record.get('Crop') or 'Unknown'
                
                # Handle NA, null, and non-numeric values
                try:
                    production = float(record.get('production_') or record.get('Production') or 0)
                except (ValueError, TypeError):
                    production = 0
                
                try:
                    area = float(record.get('area_') or record.get('Area') or 0)
                except (ValueError, TypeError):
                    area = 0
                
                district = record.get('district_name') or record.get('District_Name') or 'Unknown'

                if production > 0:  # Only count records with actual production
                    crop_totals[crop_name]['production'] += production
                    crop_totals[crop_name]['area'] += area
                    if district not in crop_totals[crop_name]['districts']:
                        crop_totals[crop_name]['districts'].append(district)

            # Convert to list and sort by production
            crops_list: List[Dict[str, Any]] = []
            for crop_name, data in crop_totals.items():
                crops_list.append({
                    'crop': crop_name,
                    'production': data['production'],
                    'area': data['area'],
                    'yield': data['production'] / data['area'] if data['area'] > 0 else 0,
                    'districts_count': len(data['districts'])
                })

            # Sort by production descending and take top N
            crops_list.sort(key=lambda x: x['production'], reverse=True)
            top_crops = crops_list[:top_n]

            year_range_str = str(year_end) if year_start == year_end else f"{year_start}-{year_end}"
            results[state] = {
                'top_crops': top_crops,
                'year': year_range_str,
                'year_start': year_start,
                'year_end': year_end,
                'total_records': len(crop_data)
            }

            print(f"Top {top_n} crops for {state}:")
            for i, crop in enumerate(top_crops, 1):
                print(f"  {i}. {crop['crop']}: {crop['production']} tonnes")

            # Add source entry for this state
            year_range_str = str(year_end) if year_start == year_end else f"{year_start}-{year_end}"
            sources.append({
                'dataset': CROP_DATASET_NAME,
                'url': f"{self.data_client.base_url}{self.data_client.CROP_PRODUCTION_RESOURCE_ID}",
                'resource_id': self.data_client.CROP_PRODUCTION_RESOURCE_ID,
                'filters_applied': {
                    'state_name': state,
                    'crop_year': year_range_str
                },
                'records_retrieved': len(crop_data)
            })

        # Convert results to expected format
        comparisons: List[Dict[str, Any]] = []
        for state, state_data in results.items():
            for crop in state_data.get('top_crops', []):
                comparisons.append({
                    'state': state,
                    'crop': crop['crop'],
                    'production': crop['production'],
                    'area': crop['area'],
                    'yield': crop['yield'],
                    'year': state_data.get('year')
                })

        return {
            'answer': '',
            'sources': sources,
            'data': {
                'states': results,
                'comparisons': comparisons,
                'statistics': comparisons,  # For LLM context
            },
            'metadata': {
                'data_sources_queried': len(sources),
                'total_records_processed': total_records,
                'states_queried': len(states),
                'year_start': year_start,
                'year_end': year_end,
                'top_n': top_n,
            },
        }

    def _identify_district(self, parsed: Mapping[str, Any]) -> Dict[str, Any]:
        """Identify the highest/lowest producing district for a crop and year."""
        states: List[str] = [str(s) for s in (parsed.get("states") or [])]
        crops: List[str] = [str(c) for c in (parsed.get("crops") or [])]
        years = parsed.get("years") or []
        year: Optional[int] = int(years[0]) if len(years) == 2 and years[0] == years[1] else (int(years[0]) if len(years) == 1 else None)
        top_n = int(parsed.get("top_n") or 5)

        crop = crops[0] if crops else None
        if not crop or not year:
            msg = "Please specify a crop and a single year to identify top/bottom districts."
            logger.warning(msg)
            return {
                "answer": msg,
                "sources": [],
                "data": {"states": {}, "comparisons": {}, "statistics": {}},
                "metadata": {"data_sources_queried": 0, "total_records_processed": 0},
            }

        fetched: List[Dict[str, Any]] = []
        sources: List[Dict[str, Any]] = []
        total_records = 0

        target_states = states or [None]
        for st in target_states:
            records = self.data_client.fetch_crop_production(
                state=st, district=None, crop=crop, year=year, season=None, limit=10000
            )
            if not records:
                continue
            fetched.extend(records)
            total_records += len(records)
            filters_applied = {"state_name": st, "crop": crop, "crop_year": year}
            filters_applied = {k: v for k, v in filters_applied.items() if v is not None}
            sources.append(
                {
                    "dataset": CROP_DATASET_NAME,
                    "url": f"{self.data_client.base_url}{self.data_client.CROP_PRODUCTION_RESOURCE_ID}",
                    "resource_id": self.data_client.CROP_PRODUCTION_RESOURCE_ID,
                    "filters_applied": filters_applied,
                    "records_retrieved": len(records),
                }
            )

        if not fetched:
            msg = "No districts found for the specified crop/year filters."
            logger.warning(msg)
            return {
                "answer": msg,
                "sources": sources,
                "data": {"states": {}, "comparisons": {}, "statistics": {}},
                "metadata": {
                    "data_sources_queried": len(sources),
                    "total_records_processed": total_records,
                },
            }

        aggregated = self._aggregate_crop_production(fetched, group_by="district_name")
        rankings = sorted(aggregated.values(), key=lambda x: x["production"], reverse=True)
        top = rankings[: top_n]
        bottom = rankings[-top_n:][::-1]

        result = {
            "answer": "",
            "sources": sources,
            "data": {
                "states": {},
                "comparisons": {"top": top, "bottom": bottom},
                "statistics": rankings,
            },
            "metadata": {
                "data_sources_queried": len(sources),
                "total_records_processed": total_records,
            },
        }
        return result

    def _analyze_trend(self, parsed: Mapping[str, Any]) -> Dict[str, Any]:
        """Analyze production trend over a specified year range for a state/crop."""
        states: List[str] = [str(s) for s in (parsed.get("states") or [])]
        crops: List[str] = [str(c) for c in (parsed.get("crops") or [])]
        years = parsed.get("years") or []
        if len(years) == 2:
            year_start, year_end = int(years[0]), int(years[1])
        else:
            year_start = year_end = None  # fetch best-effort

        state = states[0] if states else None
        crop = crops[0] if crops else None
        if not state or not crop or year_start is None or year_end is None:
            msg = "Please specify state, crop, and a valid year range for trend analysis."
            logger.warning(msg)
            return {
                "answer": msg,
                "sources": [],
                "data": {"states": {}, "comparisons": {}, "statistics": {}},
                "metadata": {"data_sources_queried": 0, "total_records_processed": 0},
            }

        fetched: List[Dict[str, Any]] = []
        sources: List[Dict[str, Any]] = []
        total_records = 0
        for y in range(year_start, year_end + 1):
            r = self.data_client.fetch_crop_production(state=state, crop=crop, year=y, season=None, limit=10000)
            if r:
                fetched.extend(r)
                total_records += len(r)
        sources.append(
            {
                "dataset": CROP_DATASET_NAME,
                "url": f"{self.data_client.base_url}{self.data_client.CROP_PRODUCTION_RESOURCE_ID}",
                "resource_id": self.data_client.CROP_PRODUCTION_RESOURCE_ID,
                "filters_applied": {"state_name": state, "crop": crop, "Years": f"{year_start}-{year_end}"},
                "records_retrieved": total_records,
            }
        )

        if not fetched:
            msg = "No production data found for the specified trend filters."
            logger.warning(msg)
            return {
                "answer": msg,
                "sources": sources,
                "data": {"states": {}, "comparisons": {}, "statistics": {}},
                "metadata": {
                    "data_sources_queried": len(sources),
                    "total_records_processed": total_records,
                },
            }

        # Aggregate by year for production and area
        by_year: Dict[int, Dict[str, float]] = defaultdict(lambda: {"production": 0.0, "area": 0.0})
        for rec in fetched:
            try:
                y = int(rec.get("crop_year") or rec.get("Crop_Year"))
            except Exception:  # noqa: BLE001
                continue
            
            # Handle NA, null, and non-numeric values
            try:
                prod = float(rec.get("production_") or rec.get("Production") or 0)
            except (ValueError, TypeError):
                prod = 0
            
            try:
                area = float(rec.get("area_") or rec.get("Area") or 0)
            except (ValueError, TypeError):
                area = 0
            
            if prod > 0:  # Only count records with actual production
                by_year[y]["production"] += prod
                by_year[y]["area"] += area

        years_sorted = sorted(by_year.keys())
        timeline: List[Dict[str, Any]] = []
        for i, y in enumerate(years_sorted):
            prod = by_year[y]["production"]
            area = by_year[y]["area"]
            yield_val = (prod / area) if area else None
            yoy = None
            if i > 0:
                prev = by_year[years_sorted[i - 1]]["production"]
                if prev:
                    yoy = ((prod - prev) / prev) * 100
            timeline.append({"year": y, "production": prod, "area": area, "yield": yield_val, "yoy_change_pct": yoy})

        # Determine trend: compute CAGR if endpoints available
        growth_rate_pct = None
        trend_direction = "stable"
        if years_sorted:
            first, last = years_sorted[0], years_sorted[-1]
            v0, v1 = by_year[first]["production"], by_year[last]["production"]
            n_years = max(1, last - first)
            if v0 > 0 and v1 > 0:
                cagr = (v1 / v0) ** (1 / n_years) - 1
                growth_rate_pct = cagr * 100
                if cagr > 0.01:
                    trend_direction = "increasing"
                elif cagr < -0.01:
                    trend_direction = "decreasing"

        result = {
            "answer": "",
            "sources": sources,
            "data": {
                "states": {state: {"timeline": timeline}},
                "comparisons": {},
                "statistics": {"trend_direction": trend_direction, "growth_rate_pct": growth_rate_pct},
            },
            "metadata": {
                "data_sources_queried": len(sources),
                "total_records_processed": total_records,
            },
        }
        return result

    def _correlate_climate_crop(self, parsed: Mapping[str, Any]) -> Dict[str, Any]:
        """Correlate rainfall with crop production"""
        
        states = parsed.get('states', [])
        crop_types = parsed.get('crop_types', [])
        years = parsed.get('years', [])
        top_n = parsed.get('top_n', 5)
        
        print(f"\n{'='*50}")
        print(f"CORRELATION ANALYSIS")
        print(f"States: {states}")
        print(f"Crop Types: {crop_types}")
        print(f"Years: {years}")
        print(f"{'='*50}\n")
        
        # Adjust years for each data type
        crop_years = self._get_available_year_range(years, 'crop')
        rainfall_years = self._get_available_year_range(years, 'rainfall')
        
        crop_year_start, crop_year_end = crop_years[0], crop_years[1]
        rainfall_year_start, rainfall_year_end = rainfall_years[0], rainfall_years[1]
        
        results = {}
        
        for state in states:
            print(f"\n--- Processing {state} ---")
            
            # 1. Fetch rainfall data
            subdivisions = self._find_matching_subdivisions(state)
            print(f"Subdivisions for {state}: {subdivisions}")
            
            all_rainfall_data = []
            for subdivision in subdivisions:
                rainfall_data = self.data_client.fetch_rainfall_data(
                    subdivision=subdivision,
                    year_start=rainfall_year_start,
                    year_end=rainfall_year_end,
                    limit=10000
                )
                all_rainfall_data.extend(rainfall_data)
            
            print(f"Rainfall records: {len(all_rainfall_data)}")
            
            # Calculate average rainfall
            avg_rainfall = 0
            if all_rainfall_data:
                total = sum(float(r.get('annual', 0) or 0) for r in all_rainfall_data)
                avg_rainfall = total / len(all_rainfall_data)
            
            # 2. Fetch crop production data
            crop_data = self.data_client.fetch_crop_production(
                state=state,
                year=None,  # Get all years in range
                limit=10000
            )
            
            print(f"Crop records: {len(crop_data)}")
            
            # Filter by year range and crop types
            filtered_crops = []
            for record in crop_data:
                year = record.get('crop_year')
                crop = record.get('crop', '')
                
                if year and crop_year_start <= year <= crop_year_end:
                    # Check if crop matches requested type
                    if crop_types:
                        if self._is_crop_type_match(crop, crop_types):
                            filtered_crops.append(record)
                    else:
                        filtered_crops.append(record)
            
            print(f"Filtered crops: {len(filtered_crops)}")
            
            # Group by crop and sum production
            crop_totals = defaultdict(lambda: {'production': 0, 'area': 0})
            
            for record in filtered_crops:
                crop_name = record.get('crop', 'Unknown')
                
                # Handle NA, null, and non-numeric values
                try:
                    production = float(record.get('production_', 0) or 0)
                except (ValueError, TypeError):
                    production = 0
                
                try:
                    area = float(record.get('area_', 0) or 0)
                except (ValueError, TypeError):
                    area = 0
                
                if production > 0:  # Only count records with actual production
                    crop_totals[crop_name]['production'] += production
                    crop_totals[crop_name]['area'] += area
            
            # Get top N crops
            crops_list = [
                {
                    'crop': crop,
                    'production': data['production'],
                    'area': data['area'],
                    'yield': data['production'] / data['area'] if data['area'] > 0 else 0
                }
                for crop, data in crop_totals.items()
            ]
            
            crops_list.sort(key=lambda x: x['production'], reverse=True)
            top_crops = crops_list[:top_n]
            
            print(f"Top {top_n} crops for {state}:")
            for i, crop in enumerate(top_crops, 1):
                print(f"  {i}. {crop['crop']}: {crop['production']:.0f} tonnes")
            
            results[state] = {
                'average_rainfall': avg_rainfall,
                'rainfall_year_range': f"{rainfall_year_start}-{rainfall_year_end}",
                'rainfall_data_points': len(all_rainfall_data),
                'top_crops': top_crops,
                'crop_year_range': f"{crop_year_start}-{crop_year_end}",
                'crop_data_points': len(filtered_crops),
                'subdivisions': subdivisions
            }
        
        # Prepare sources
        sources = []
        
        if any(r['rainfall_data_points'] > 0 for r in results.values()):
            sources.append({
                'dataset': 'Area-weighted Rainfall Data (36 Meteorological Subdivisions)',
                'url': 'https://api.data.gov.in/resource/440dbca7-86ce-4bf6-b1af-83af2855757e',
                'resource_id': '440dbca7-86ce-4bf6-b1af-83af2855757e',
                'filters_applied': {
                    'year_range': f"{rainfall_year_start}-{rainfall_year_end}",
                    'subdivisions': list(set([sub for r in results.values() for sub in r.get('subdivisions', [])]))
                },
                'records_retrieved': sum(r['rainfall_data_points'] for r in results.values())
            })
        
        if any(r['crop_data_points'] > 0 for r in results.values()):
            sources.append({
                'dataset': 'District-wise Crop Production',
                'url': 'https://api.data.gov.in/resource/35be999b-0208-4354-b557-f6ca9a5355de',
                'resource_id': '35be999b-0208-4354-b557-f6ca9a5355de',
                'filters_applied': {
                    'states': states,
                    'year_range': f"{crop_year_start}-{crop_year_end}",
                    'crop_types': crop_types if crop_types else 'All'
                },
                'records_retrieved': sum(r['crop_data_points'] for r in results.values())
            })
        
        return {
            'answer': '',
            'data': results,
            'sources': sources,
            'metadata': {
                'states_analyzed': len(states),
                'rainfall_period': f"{rainfall_year_start}-{rainfall_year_end}",
                'crop_period': f"{crop_year_start}-{crop_year_end}",
                'top_n': top_n
            }
        }

    def _policy_analysis(self, parsed: Mapping[str, Any]) -> Dict[str, Any]:
        """Generate policy recommendations using data context (rainfall + production)."""
        states: List[str] = [str(s) for s in (parsed.get("states") or [])]
        crops: List[str] = [str(c) for c in (parsed.get("crops") or [])]
        years = parsed.get("years") or []
        year_start: Optional[int] = int(years[0]) if len(years) == 2 else None
        year_end: Optional[int] = int(years[1]) if len(years) == 2 else None

        state = states[0] if states else None
        crop = crops[0] if crops else None

        sources: List[Dict[str, Any]] = []
        total_records = 0
        rainfall_context: Dict[str, Any] = {}
        production_context: Dict[str, Any] = {}

        if state and year_start and year_end:
            subdivisions = self._find_matching_subdivisions(state)
            rainfall_stats: List[Dict[str, Any]] = []
            for subdiv in subdivisions:
                r = self.data_client.fetch_rainfall_data(subdivision=subdiv, year_start=year_start, year_end=year_end, limit=2000)
                total_records += len(r)
                rainfall_stats.extend(r)
                sources.append(
                    {
                        "dataset": RAIN_DATASET_NAME,
                        "url": f"{self.data_client.base_url}{self.data_client.RAINFALL_RESOURCE_ID}",
                        "resource_id": self.data_client.RAINFALL_RESOURCE_ID,
                        "filters_applied": {"Subdivision": subdiv, "Years": f"{year_start}-{year_end}"},
                        "records_retrieved": len(r),
                    }
                )
            rainfall_context = {"state": state, "years": [year_start, year_end], "records": len(rainfall_stats)}

        if state and crop and year_start and year_end:
            prod_records: List[Dict[str, Any]] = []
            for y in range(year_start, year_end + 1):
                r = self.data_client.fetch_crop_production(state=state, crop=crop, year=y, season=None, limit=10000)
                prod_records.extend(r)
                total_records += len(r)
            sources.append(
                {
                    "dataset": CROP_DATASET_NAME,
                    "url": f"{self.data_client.base_url}{self.data_client.CROP_PRODUCTION_RESOURCE_ID}",
                    "resource_id": self.data_client.CROP_PRODUCTION_RESOURCE_ID,
                    "filters_applied": {"state_name": state, "crop": crop, "Years": f"{year_start}-{year_end}"},
                    "records_retrieved": len(prod_records),
                }
            )
            production_context = {"state": state, "crop": crop, "years": [year_start, year_end], "records": len(prod_records)}

        data_stub = {
            "rainfall": rainfall_context,
            "production": production_context,
        }

        result = {
            "answer": "",
            "sources": sources,
            "data": {"states": {}, "comparisons": {}, "statistics": data_stub},
            "metadata": {
                "data_sources_queried": len(sources),
                "total_records_processed": total_records,
            },
        }
        return result

    # -------------------- Helpers --------------------
    def _prepare_data_context(self, results: Mapping[str, Any]) -> Mapping[str, Any]:
        """Prepare a compact context object for LLM grounding and citations."""
        sources = results.get("sources") or []
        # Flatten sources to strings for readability
        source_strings: List[str] = []
        for s in sources:
            ds = s.get("dataset") or ""
            filt = s.get("filters_applied") or {}
            recs = s.get("records_retrieved") or 0
            source_strings.append(
                f"{ds} | Filters: " + ", ".join(f"{k}={v}" for k, v in filt.items()) + f" | Records: {recs}"
            )
        return {
            "sources": source_strings,
            "notes": "Data pulled from data.gov.in official resources.",
        }

    def _calculate_rainfall_stats(self, data: List[Mapping[str, Any]]) -> Mapping[str, Any]:
        """Compute statistics for rainfall series.

        Expects items with keys: 'year' and 'annual'. Returns mean, median, min/max,
        standard deviation, and wettest/driest years.
        """
        series = [float(d.get("annual")) for d in data if _is_number(d.get("annual"))]
        if not series:
            return {}

        n = len(series)
        mean = sum(series) / n
        sorted_vals = sorted(series)
        if n % 2 == 1:
            median = sorted_vals[n // 2]
        else:
            median = (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
        min_val = min(series)
        max_val = max(series)
        variance = sum((x - mean) ** 2 for x in series) / n
        stddev = math.sqrt(variance)

        # Find wettest/driest years
        wettest = max(data, key=lambda d: float(d.get("annual") or -1))
        driest = min(data, key=lambda d: float(d.get("annual") or float("inf")))

        # Simple trend based on endpoints
        trend = None
        if len(data) >= 2:
            d_sorted = sorted(data, key=lambda d: int(d.get("year")))
            first, last = d_sorted[0], d_sorted[-1]
            if _is_number(first.get("annual")) and _is_number(last.get("annual")):
                delta = float(last["annual"]) - float(first["annual"])
                trend = "increasing" if delta > 0 else ("decreasing" if delta < 0 else "stable")

        return {
            "mean": mean,
            "median": median,
            "min": min_val,
            "max": max_val,
            "stddev": stddev,
            "wettest_year": wettest.get("year"),
            "driest_year": driest.get("year"),
            "trend": trend,
        }

    def _aggregate_crop_production(self, data: List[Mapping[str, Any]], *, group_by: str) -> Dict[str, Dict[str, Any]]:
        """Aggregate crop production/area by a specific field and compute yield.

        group_by: one of 'state_name', 'district_name', 'crop'.
        """
        agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"production": 0.0, "area": 0.0})
        for rec in data:
            key = str(rec.get(group_by) or "Unknown")
            
            # Handle NA, null, and non-numeric values
            try:
                prod = float(rec.get("production_") or rec.get("Production") or 0)
            except (ValueError, TypeError):
                prod = 0
            
            try:
                area = float(rec.get("area_") or rec.get("Area") or 0)
            except (ValueError, TypeError):
                area = 0
            
            if prod > 0:  # Only count records with actual production
                agg[key]["production"] += prod
                agg[key]["area"] += area

        out: Dict[str, Dict[str, Any]] = {}
        for key, vals in agg.items():
            area = vals["area"]
            yield_val = (vals["production"] / area) if area else None
            out[key] = {
                group_by: key,
                "production": vals["production"],
                "area": area,
                "yield": yield_val,
            }
        return out

    def _get_available_year_range(self, query_years: List[int], data_type: str = 'crop') -> List[int]:
        """Convert requested years to available years in the dataset.

        Args:
            query_years: [start_year, end_year] from parsed query
            data_type: 'crop' or 'rainfall'

        Returns:
            [start_year, end_year] adjusted to available data
        """
        # Latest available years in our datasets
        CROP_DATA_END = 2005
        RAINFALL_DATA_END = 2015

        if data_type == 'crop':
            max_year = CROP_DATA_END
        else:
            max_year = RAINFALL_DATA_END

        if not query_years or len(query_years) == 0:
            # Default to recent range
            return [max_year - 4, max_year]

        end_year = query_years[1] if len(query_years) > 1 else query_years[0]
        start_year = query_years[0] if len(query_years) > 1 else query_years[0]

        # If requested end year is beyond available data, adjust to latest available
        if end_year > max_year:
            year_range = end_year - start_year
            end_year = max_year
            start_year = max(1997, end_year - year_range)

        return [start_year, end_year]

    def _is_crop_type_match(self, crop_name: str, crop_types: List[str]) -> bool:
        """Check if a crop belongs to the specified crop type"""
        
        crop_name_lower = crop_name.lower()
        
        cereals = ['rice', 'wheat', 'maize', 'bajra', 'jowar', 'ragi', 'barley', 'other cereals']
        pulses = ['arhar', 'tur', 'moong', 'urad', 'masoor', 'gram', 'lentil', 'peas', 'other pulses']
        oilseeds = ['groundnut', 'sunflower', 'soyabean', 'rapeseed', 'mustard', 'safflower', 'niger', 'sesamum']
        cash_crops = ['cotton', 'sugarcane', 'tobacco', 'jute']
        
        for crop_type in crop_types:
            if crop_type.lower() == 'cereal':
                if any(c in crop_name_lower for c in cereals):
                    return True
            elif crop_type.lower() == 'pulse':
                if any(c in crop_name_lower for c in pulses):
                    return True
            elif crop_type.lower() == 'oilseed':
                if any(c in crop_name_lower for c in oilseeds):
                    return True
            elif crop_type.lower() == 'cash crop':
                if any(c in crop_name_lower for c in cash_crops):
                    return True
        
        return False

    def _find_matching_subdivisions(self, state: str) -> List[str]:
        """Return list of subdivisions that include the specified state."""
        state_std = self.data_client.standardize_state_name(state)
        matches: List[str] = []
        for subdiv, states in self.data_client.SUBDIVISION_TO_STATES.items():
            if state_std in states:
                matches.append(subdiv)
        return matches


def _is_number(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _pearson_correlation(xs: List[float], ys: List[float]) -> Optional[float]:
    n = min(len(xs), len(ys))
    if n < 2:
        return None
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
    den_x = math.sqrt(sum((x - x_mean) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - y_mean) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


