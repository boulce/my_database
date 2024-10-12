package org.example.metadata;

public class AttributeMetadata {
    private String relationName;
    private String attributeName;
    private String domainType;
    private int position;
    private int length;
    private boolean isPrimary;
    private String referenceRelationName;
    private String referenceAttributeName;

    public AttributeMetadata(String relationName, String attributeName, String domainType, int position, int length, boolean isPrimary, String referenceRelationName, String referenceAttributeName) {
        this.relationName = relationName;
        this.attributeName = attributeName;
        this.domainType = domainType;
        this.position = position;
        this.length = length;
        this.isPrimary = isPrimary;
        this.referenceRelationName = referenceRelationName;
        this.referenceAttributeName = referenceAttributeName;
    }

    public String getRelationName() {
        return relationName;
    }

    public void setRelationName(String relationName) {
        this.relationName = relationName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public String getDomainType() {
        return domainType;
    }

    public void setDomainType(String domainType) {
        this.domainType = domainType;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public String getReferenceRelationName() {
        return referenceRelationName;
    }

    public void setReferenceRelationName(String referenceRelationName) {
        this.referenceRelationName = referenceRelationName;
    }

    public String getReferenceAttributeName() {
        return referenceAttributeName;
    }

    public void setReferenceAttributeName(String referenceAttributeName) {
        this.referenceAttributeName = referenceAttributeName;
    }
}
