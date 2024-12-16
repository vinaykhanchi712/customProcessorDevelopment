package org.example;



import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

public class TransactionProcessor extends AbstractProcessor {

    public static final PropertyDescriptor THRESHOLD = new PropertyDescriptor.Builder()
            .name("Transaction Threshold")
            .description("The monetary threshold for flagging fraudulent transactions.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_FRAUD = new Relationship.Builder()
            .name("fraud")
            .description("Transactions identified as fraudulent.")
            .build();

    public static final Relationship REL_NON_FRAUD = new Relationship.Builder()
            .name("non-fraud")
            .description("Transactions identified as non-fraudulent.")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = List.of(THRESHOLD);

        this.relationships = Set.of(REL_FRAUD, REL_NON_FRAUD);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int threshold = context.getProperty(THRESHOLD).asInteger();


        Map<String, String> attributes = flowFile.getAttributes();
        String transactionAmountStr = attributes.get("transaction.amount");
        String transactionCategory = attributes.get("transaction.category");

        if (transactionAmountStr != null) {
            try {
                double transactionAmount = Double.parseDouble(transactionAmountStr);

                if (transactionAmount > threshold) {

                    flowFile = session.putAttribute(flowFile, "transaction.status", "fraud");
                    session.transfer(flowFile, REL_FRAUD);
                } else {

                    flowFile = session.putAttribute(flowFile, "transaction.status", "non-fraud");
                    session.transfer(flowFile, REL_NON_FRAUD);
                }

            } catch (NumberFormatException e) {
                getLogger().error("Failed to parse transaction amount: {}", transactionAmountStr, e);
                session.remove(flowFile);
            }
        } else {
            getLogger().warn("Transaction amount attribute is missing.");
            session.remove(flowFile);
        }
    }
}
