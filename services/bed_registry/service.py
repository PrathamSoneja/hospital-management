"""
Domain service implementation for Bed Registry Service.
Owned by Member D (Jay Patel).

Enforces:
1. Bed allocation & release lifecycle
2. ZooKeeper fencing token validation: rejects any write carrying a stale fencing token
3. Idempotent execution of mutation operations based on operation_id
4. Ownership protection: only the allocating admission can release its bed
"""
import logging
from typing import List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from services.bed_registry.models import (
    BedModel, PoolFencingTokenModel, BedOperationModel, BedDto
)
from shared.observability.metrics import STALE_FENCING_REJECTIONS_TOTAL

logger = logging.getLogger("bed-registry-service")

class BedRegistryDomainService:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def list_available_beds(self, pool_id: str, request_id: str = "") -> List[BedDto]:
        """Returns all beds in a pool with status AVAILABLE."""
        with self.session_factory() as session:
            stmt = select(BedModel).where(
                and_(
                    BedModel.pool_id == pool_id,
                    BedModel.status == "AVAILABLE"
                )
            ).order_by(BedModel.bed_id)
            beds = session.scalars(stmt).all()
            return [
                BedDto(
                    bed_id=b.bed_id,
                    pool_id=b.pool_id,
                    ward=b.ward,
                    status=b.status,
                    current_allocation_id=b.current_allocation_id
                )
                for b in beds
            ]

    def get_bed(self, bed_id: str, request_id: str = "") -> Optional[BedDto]:
        """Fetches bed details by ID."""
        with self.session_factory() as session:
            bed = session.get(BedModel, bed_id)
            if not bed:
                return None
            return BedDto(
                bed_id=bed.bed_id,
                pool_id=bed.pool_id,
                ward=bed.ward,
                status=bed.status,
                current_allocation_id=bed.current_allocation_id
            )

    def create_or_update_bed(self, bed_id: str, pool_id: str, ward: str, status: str = "AVAILABLE") -> BedDto:
        """Admin/seed method to register a bed."""
        with self.session_factory() as session:
            bed = session.get(BedModel, bed_id)
            if bed is None:
                bed = BedModel(
                    bed_id=bed_id,
                    pool_id=pool_id,
                    ward=ward,
                    status=status
                )
                session.add(bed)
            else:
                bed.pool_id = pool_id
                bed.ward = ward
                bed.status = status
            session.commit()
            session.refresh(bed)
            return BedDto(
                bed_id=bed.bed_id,
                pool_id=bed.pool_id,
                ward=bed.ward,
                status=bed.status,
                current_allocation_id=bed.current_allocation_id
            )

    def get_highest_fencing_token(self, pool_id: str) -> int:
        """Returns highest fencing token observed for the pool."""
        with self.session_factory() as session:
            token_record = session.get(PoolFencingTokenModel, pool_id)
            return token_record.highest_fencing_token if token_record else 0

    def allocate_bed(
        self,
        operation_id: str,
        pool_id: str,
        bed_id: str,
        allocation_id: str,
        fencing_token: int,
        request_id: str = ""
    ) -> Tuple[bool, Optional[BedDto], str]:
        """
        Allocates a bed under distributed lock fencing.

        Fencing Rules:
        - If fencing_token < current_highest: REJECT as STALE.
        - If fencing_token >= current_highest: ACCEPT and update highest.
        - Idempotency: repeated operation_id returns previous successful outcome.
        """
        with self.session_factory() as session:
            # 1. Idempotency Check
            prior_op = session.get(BedOperationModel, operation_id)
            if prior_op:
                bed = session.get(BedModel, prior_op.bed_id)
                if bed and bed.current_allocation_id == allocation_id:
                    logger.info("bed_allocate_idempotent_replay", extra={
                        "event": "bed_allocate_idempotent_replay",
                        "operation_id": operation_id,
                        "bed_id": bed_id,
                        "request_id": request_id
                    })
                    return True, BedDto(
                        bed_id=bed.bed_id,
                        pool_id=bed.pool_id,
                        ward=bed.ward,
                        status=bed.status,
                        current_allocation_id=bed.current_allocation_id
                    ), ""

            # 2. Check & Update Fencing Token
            token_record = session.get(PoolFencingTokenModel, pool_id, with_for_update=True)
            current_highest = token_record.highest_fencing_token if token_record else 0

            if fencing_token < current_highest:
                STALE_FENCING_REJECTIONS_TOTAL.labels(service="bed-registry", pool_id=pool_id).inc()
                msg = f"Stale fencing token {fencing_token}; pool {pool_id} highest token is {current_highest}"
                logger.warning("bed_allocation_stale_fencing_rejected", extra={
                    "event": "bed_allocation_stale_fencing_rejected",
                    "pool_id": pool_id,
                    "fencing_token": fencing_token,
                    "current_highest": current_highest,
                    "request_id": request_id
                })
                return False, None, msg

            # Update highest fencing token seen
            if token_record is None:
                token_record = PoolFencingTokenModel(pool_id=pool_id, highest_fencing_token=fencing_token)
                session.add(token_record)
            else:
                if fencing_token > token_record.highest_fencing_token:
                    token_record.highest_fencing_token = fencing_token

            # 3. Check Bed Availability
            bed = session.get(BedModel, bed_id, with_for_update=True)
            if not bed:
                return False, None, f"Bed {bed_id} not found"

            if bed.pool_id != pool_id:
                return False, None, f"Bed {bed_id} belongs to pool {bed.pool_id}, not {pool_id}"

            if bed.status != "AVAILABLE" and bed.current_allocation_id != allocation_id:
                return False, None, f"Bed {bed_id} already occupied by {bed.current_allocation_id}"

            # 4. Mutate State
            bed.status = "OCCUPIED"
            bed.current_allocation_id = allocation_id

            # 5. Record Operation for Idempotency
            op = BedOperationModel(
                operation_id=operation_id,
                operation_type="ALLOCATE",
                bed_id=bed_id,
                allocation_id=allocation_id,
                fencing_token=fencing_token
            )
            session.add(op)
            session.commit()
            session.refresh(bed)

            logger.info("bed_allocated_successfully", extra={
                "event": "bed_allocated_successfully",
                "bed_id": bed.bed_id,
                "pool_id": pool_id,
                "allocation_id": allocation_id,
                "fencing_token": fencing_token,
                "request_id": request_id
            })

            return True, BedDto(
                bed_id=bed.bed_id,
                pool_id=bed.pool_id,
                ward=bed.ward,
                status=bed.status,
                current_allocation_id=bed.current_allocation_id
            ), ""

    def release_bed(
        self,
        operation_id: str,
        pool_id: str,
        bed_id: str,
        allocation_id: str,
        fencing_token: int,
        request_id: str = ""
    ) -> Tuple[bool, str]:
        """
        Releases an occupied bed under distributed lock fencing.

        Fencing Rules:
        - If fencing_token < current_highest: REJECT as STALE.
        - Verifies ownership: bed must currently be assigned to allocation_id.
        """
        with self.session_factory() as session:
            # 1. Idempotency Check
            prior_op = session.get(BedOperationModel, operation_id)
            if prior_op:
                return True, ""

            # 2. Check Fencing Token
            token_record = session.get(PoolFencingTokenModel, pool_id, with_for_update=True)
            current_highest = token_record.highest_fencing_token if token_record else 0

            if fencing_token < current_highest:
                STALE_FENCING_REJECTIONS_TOTAL.labels(service="bed-registry", pool_id=pool_id).inc()
                msg = f"Stale fencing token {fencing_token}; pool {pool_id} highest token is {current_highest}"
                logger.warning("bed_release_stale_fencing_rejected", extra={
                    "event": "bed_release_stale_fencing_rejected",
                    "pool_id": pool_id,
                    "fencing_token": fencing_token,
                    "current_highest": current_highest,
                    "request_id": request_id
                })
                return False, msg

            if token_record is None:
                token_record = PoolFencingTokenModel(pool_id=pool_id, highest_fencing_token=fencing_token)
                session.add(token_record)
            else:
                if fencing_token > token_record.highest_fencing_token:
                    token_record.highest_fencing_token = fencing_token

            # 3. Check Bed & Ownership
            bed = session.get(BedModel, bed_id, with_for_update=True)
            if not bed:
                return False, f"Bed {bed_id} not found"

            if bed.current_allocation_id != allocation_id:
                return False, f"Bed {bed_id} is owned by {bed.current_allocation_id}, cannot be released by {allocation_id}"

            # 4. Mutate State
            bed.status = "AVAILABLE"
            bed.current_allocation_id = None

            # 5. Record Operation
            op = BedOperationModel(
                operation_id=operation_id,
                operation_type="RELEASE",
                bed_id=bed_id,
                allocation_id=allocation_id,
                fencing_token=fencing_token
            )
            session.add(op)
            session.commit()

            logger.info("bed_released_successfully", extra={
                "event": "bed_released_successfully",
                "bed_id": bed.bed_id,
                "pool_id": pool_id,
                "allocation_id": allocation_id,
                "fencing_token": fencing_token,
                "request_id": request_id
            })

            return True, ""

