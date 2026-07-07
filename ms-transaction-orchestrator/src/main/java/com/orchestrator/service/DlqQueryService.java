package com.orchestrator.service;

import org.springframework.data.domain.*;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.orchestrator.entity.DlqEvent;
import com.orchestrator.repository.DlqEventRepository;
import com.orchestrator.specification.DlqEventSpecification;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DlqQueryService {

    private final DlqEventRepository repository;

    public Page<DlqEvent> search(
            String transactionId,
            String eventType,
            Boolean replayed,
            int page,
            int size
    ) {

        Specification<DlqEvent> spec = Specification
                .where(DlqEventSpecification.hasTransactionId(transactionId))
                .and(DlqEventSpecification.hasEventType(eventType))
                .and(DlqEventSpecification.isReplayed(replayed));

        Pageable pageable = PageRequest.of(page, size, Sort.by("createdAt").descending());

        return repository.findAll(spec, pageable);
    }
}