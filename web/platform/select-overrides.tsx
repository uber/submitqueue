"use client";

import { StyledInput, type SelectOverrides } from "baseui/select";
import { forwardRef, useState, type ComponentPropsWithoutRef } from "react";

// Closing removes the options; suppress the old ID until the menu reports a new one.
const SelectInput = forwardRef<HTMLInputElement, ComponentPropsWithoutRef<typeof StyledInput>>((props, ref) => {
  const activeDescendant = props["aria-activedescendant"];
  const [closedActiveDescendant, setClosedActiveDescendant] = useState(activeDescendant);
  if (!props.$isOpen && closedActiveDescendant !== activeDescendant) setClosedActiveDescendant(activeDescendant);
  return <StyledInput {...props} ref={ref}
    aria-activedescendant={props.$isOpen && activeDescendant !== closedActiveDescendant ? activeDescendant : undefined} />;
});

export const selectOverrides: SelectOverrides = {
  Input: { component: SelectInput },
  // The enclosing option owns selection semantics; its label is a generic div.
  OptionContent: { props: { "aria-selected": undefined, "aria-readonly": undefined } },
};
