Schema created with [diagrams.net](https://www.diagrams.net)

## Comments

### burgerLinker schema strategy:
- Use main stream vocabs where possible, e.g. schema, dbo
- Use more domain oriented vocabs, where needed
- Resort to civ:terms to resolve issues

We choose to stay close to terms used on the original documents, e.g. ‘mother’ or ‘father’ rather than ‘parent’ or ‘care taker’. The downside of this choice is that we make assumptions on the roles that individuals related to, without knowing whether that is the case.

### Event
- We use bio:Event rather than schema:Event has schema:Event appears to relate to events in a business or leisure context.

### Registration date
- We refrain from using the uiot:registrationDate or serif:registrationDate as these appear to be bound to specific types of registrations, services and patents respectively. We use: civ:registrationDate instead.

### Comments regarding the use of the bio ontology
    - Bio events relate to foaf:Agent not schema:Person. We use the latter as it is more main stream nowadays;
    - Bio events use ‘bio:principal’, rather than ‘civ:newborn’ or ‘civ:deceased’. We choose the latter, for convenience;
    - Bio:mother / bio:father refer to biological parents, whereas we can only assume that.
